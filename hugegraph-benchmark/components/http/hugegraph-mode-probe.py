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

"""Verify HugeGraph REST execution mode with a bounded sentinel and JVM stack.

Run inside the target Linux container, with its effective UID/GID and namespaces.
The outer controller owns Docker/build identity, exclusive access, and resource
limits. This probe performs no deployment, retry, fallback, heap scan or graph
mutation. Run before warmup and after drain, never inside a measurement window.
"""
import argparse
import datetime
import hashlib
import http.client
import ipaddress
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import uuid
import zlib

PREFIX = 'org.apache.hugegraph.api.gremlin.'
EXECUTORS = {'forward': 'ForwardGremlinRestExecutor', 'embedded': 'EmbeddedGremlinRestExecutor'}
FRAME = re.compile(r'^\s+at (?:[A-Za-z0-9_.@-]+/)?(?:[A-Za-z0-9_.@-]*/)?'
                   r'([A-Za-z0-9_.$]+)\.([A-Za-z0-9_$<>]+)\([^\r\n]*\)\s*$')
THREAD_HEADER = re.compile(r'^"[^"\r\n]*"\s+.*\btid=0x[0-9a-fA-F]+\s+.*\bnid=')


class ProbeError(Exception):
    """Only fixed, non-secret codes cross the evidence/CLI boundary."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class Cancellation:
    """Signal handlers only assign fields: no locks, Events or exceptions in a handler."""

    def __init__(self):
        self.cancelled = False
        self.code = None

    def signal(self, signum, frame):
        if self.code is None:
            self.code = 'probe-cancelled-sigterm' if signum == signal.SIGTERM else 'probe-cancelled-sigint'
        self.cancelled = True

    def check(self):
        if self.cancelled:
            raise ProbeError(self.code)


def check_cancel(cancellation):
    if cancellation is not None:
        cancellation.check()


def wait_event(event, deadline_ns, cancellation):
    while True:
        check_cancel(cancellation)
        remaining = (deadline_ns-time.monotonic_ns())/1e9
        if remaining <= 0:
            return event.is_set()
        if event.wait(min(remaining, 0.05)):
            check_cancel(cancellation)
            return True


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=True, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')


def bounded_file(path, limit):
    with Path(path).open('rb') as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ProbeError('identity-file-too-large')
    return value


def parse_endpoint(value):
    try:
        parsed = urlsplit(value)
        host = ipaddress.ip_address(parsed.hostname or '')
        port = parsed.port
    except ValueError:
        raise ProbeError('endpoint-must-be-loopback-http') from None
    if (parsed.scheme != 'http' or not host.is_loopback or not port or parsed.username is not None or
            parsed.password is not None or parsed.path != '/gremlin' or parsed.query or parsed.fragment or
            '%' in str(host)):
        raise ProbeError('endpoint-must-be-loopback-http')
    return str(host), port, parsed.path


def proc_stat(raw):
    # comm can contain spaces and closing parentheses; fields following its final ')' are numeric/state.
    tail = raw.rsplit(')', 1)[-1].split()
    if len(tail) < 20 or not tail[19].isdigit() or tail[0] in ('Z', 'X', 'x'):
        raise ProbeError('java-process-not-live')
    return tail[19]


def listener_inodes(raw, family, host, port):
    found = set()
    for line in raw.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 10 or fields[3] != '0A':
            continue
        address, encoded_port = fields[1].split(':')
        if int(encoded_port, 16) != port:
            continue
        if family == socket.AF_INET:
            address_bytes = struct.pack('=I', int(address, 16))
        else:
            address_bytes = struct.pack('=4I', *(int(address[i:i+8], 16) for i in range(0, 32, 8)))
        bind = ipaddress.ip_address(socket.inet_ntop(family, address_bytes))
        if isinstance(bind, ipaddress.IPv6Address) and bind.ipv4_mapped:
            bind = bind.ipv4_mapped
        target = ipaddress.ip_address(host)
        if bind == target or (bind.version == target.version and bind.is_unspecified):
            found.add(fields[9])
    return found


def inspect_process(pid, jcmd, endpoint, _proc_root=Path('/proc')):
    """Fail closed unless endpoint listener and JDK belong to this exact live Java process."""
    root = _proc_root / str(pid)
    host, port, _ = parse_endpoint(endpoint)
    try:
        start = proc_stat(bounded_file(root/'stat', 65536).decode('utf-8'))
        status = bounded_file(root/'status', 65536).decode('utf-8')
        euid = int(re.search(r'^Uid:\s+\d+\s+(\d+)', status, re.M).group(1))
        egid = int(re.search(r'^Gid:\s+\d+\s+(\d+)', status, re.M).group(1))
        if (euid, egid) != (os.geteuid(), os.getegid()):
            raise ProbeError('effective-user-group-mismatch')
        namespaces = {name: os.readlink(root/'ns'/name) for name in ('pid', 'net', 'mnt', 'user')}
        if any(value != os.readlink(_proc_root/'self/ns'/name) for name, value in namespaces.items()):
            raise ProbeError('process-namespace-mismatch')
        java = (root/'exe').resolve(strict=True)
        jcmd = Path(jcmd).resolve(strict=True)
        if (java.name != 'java' or jcmd.name != 'jcmd' or not jcmd.is_file() or
                not os.access(jcmd, os.X_OK) or not os.path.samefile(java, jcmd.parent/'java')):
            raise ProbeError('jcmd-must-belong-to-target-jdk')
        socket_inodes = set()
        descriptors = list((root/'fd').iterdir())
        if len(descriptors) > 65536:
            raise ProbeError('process-fd-inventory-too-large')
        for descriptor in descriptors:
            try:
                match = re.fullmatch(r'socket:\[(\d+)\]', os.readlink(descriptor))
            except FileNotFoundError:
                continue
            if match:
                socket_inodes.add(match.group(1))
        listening = set()
        for name, family in [('tcp', socket.AF_INET), ('tcp6', socket.AF_INET6)]:
            path = root/'net'/name
            if path.exists():
                listening.update(listener_inodes(bounded_file(path, 4*1024*1024).decode('ascii'),
                                                 family, host, port))
        owned = sorted(socket_inodes & listening)
        if not owned:
            raise ProbeError('endpoint-listener-not-owned-by-java-pid')
        cgroup = bounded_file(root/'cgroup', 16384).decode('utf-8')
        container_ids = sorted(set(re.findall(r'(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])', cgroup)))
        snapshot = {'pid': pid, 'startTimeTicks': start, 'effectiveUid': euid, 'effectiveGid': egid,
                    'bootId': bounded_file(_proc_root/'sys/kernel/random/boot_id', 128).decode('ascii').strip(),
                    'namespaces': namespaces, 'javaExecutable': str(java), 'jcmdExecutable': str(jcmd),
                    'javaExecutableSha256': sha256(bounded_file(java, 16*1024*1024)),
                    'jcmdExecutableSha256': sha256(bounded_file(jcmd, 16*1024*1024)),
                    'listenerSocketInodes': owned, 'cgroupSha256': sha256(cgroup.encode('utf-8')),
                    'containerIdHints': container_ids, 'hostname': socket.gethostname()}
        if start != proc_stat(bounded_file(root/'stat', 65536).decode('utf-8')):
            raise ProbeError('process-identity-changed')
        return snapshot
    except ProbeError:
        raise
    except (OSError, ValueError, AttributeError, UnicodeError, IndexError):
        raise ProbeError('process-identity-unavailable') from None


sys.dont_write_bytecode = True
_CLOCK_SPEC = importlib.util.spec_from_file_location('hgbench_jvm_clock_capture',
                                                    Path(__file__).with_name('jvm-clock-capture.py'))
CLOCK = importlib.util.module_from_spec(_CLOCK_SPEC)
_CLOCK_SPEC.loader.exec_module(CLOCK)
CLOCK_EXECUTABLE_MAX_BYTES, CLOCK_HASH_MAX_NS = CLOCK.CLOCK_EXECUTABLE_MAX_BYTES, CLOCK.CLOCK_HASH_MAX_NS
clock_file_metadata, clock_executable = CLOCK.clock_file_metadata, CLOCK.clock_executable
clock_snapshot, clock_snapshot_changes = CLOCK.clock_snapshot, CLOCK.clock_snapshot_changes


def parse_thread_dump(raw, expected):
    """Match API and executor frames in one HotSpot thread, never across threads."""
    try:
        text = raw.decode('utf-8', errors='strict')
    except UnicodeError:
        raise ProbeError('thread-dump-invalid-utf8') from None
    # The complete HotSpot Thread.print output includes this trailer. Its absence fails closed.
    if '\x00' in text or 'Full thread dump ' not in text or not re.search(r'^JNI global refs:', text, re.M):
        raise ProbeError('thread-dump-incomplete')
    sections = []
    current = None
    for line in text.splitlines():
        if line.startswith('"'):
            current = {'header': line, 'validHeader': bool(THREAD_HEADER.match(line)), 'frames': []}
            sections.append(current)
        elif line.startswith('JNI global refs:') or line.startswith('Found '):
            current = None
        elif current is not None:
            match = FRAME.match(line)
            if match:
                current['frames'].append((match.group(1), match.group(2)))
    if not sections:
        raise ProbeError('thread-dump-no-threads')
    matched = []
    for section in sections:
        frames = section['frames']
        api_positions = [index for index, value in enumerate(frames) if value == (PREFIX+'GremlinAPI', 'post')]
        executor_modes = {mode for mode, cls in EXECUTORS.items()
                          if any(name == PREFIX+cls and method in ('post', 'execute') for name, method in frames)}
        if executor_modes and not api_positions:
            raise ProbeError('executor-stack-without-rest-api')
        if not api_positions:
            continue
        if not section['validHeader'] or len(api_positions) != 1 or len(executor_modes) != 1:
            raise ProbeError('ambiguous-rest-thread-stack')
        actual = next(iter(executor_modes))
        # A real synchronous call has executor.post closer to the top than its API caller.
        executor_post = [index for index, value in enumerate(frames)
                         if value == (PREFIX+EXECUTORS[actual], 'post')]
        if len(executor_post) != 1 or executor_post[0] >= api_positions[0]:
            raise ProbeError('executor-stack-call-order-invalid')
        if actual != expected:
            raise ProbeError('actual-mode-mismatch')
        matched.append({'threadHeader': section['header'], 'mode': actual,
                        'frames': ['.'.join(value) for value in frames]})
    if len(matched) != 1:
        raise ProbeError('expected-one-active-rest-probe-thread')
    return matched[0]


def redact(raw, secret):
    # Keep byte budgets unchanged even for a short secret repeated throughout diagnostic output.
    return raw.replace(secret.encode('utf-8'), b'*'*len(secret.encode('utf-8'))) if secret else raw


def bounded_command(argv, deadline_ns, max_bytes, out, stem, secret, cancellation=None):
    started = time.monotonic_ns()
    check_cancel(cancellation)
    if started >= deadline_ns:
        raise ProbeError('command-deadline-exceeded')
    buffers = {'stdout': bytearray(), 'stderr': bytearray()}
    record = {'argv': argv, 'startedMonotonicNs': started, 'maxBytes': max_bytes}
    process = None
    selector = selectors.DefaultSelector()
    code = None
    try:
        # Do not propagate credentials, JAVA_TOOL_OPTIONS, LD_PRELOAD or other injected runtime options.
        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True,
                                   env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C', 'TMPDIR': '/tmp'})
        record['processGroupId'] = process.pid
        for name, stream in [('stdout', process.stdout), ('stderr', process.stderr)]:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map():
            check_cancel(cancellation)
            remaining = (deadline_ns-time.monotonic_ns())/1e9
            if remaining <= 0:
                raise ProbeError('command-deadline-exceeded')
            for key, _ in selector.select(min(remaining, 0.1)):
                chunk = os.read(key.fileobj.fileno(), 16384)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                available = max_bytes - sum(map(len, buffers.values()))
                buffers[key.data].extend(chunk[:max(0, available)])
                if len(chunk) > available:
                    raise ProbeError('command-output-limit-exceeded')
        while process.poll() is None:
            check_cancel(cancellation)
            remaining = (deadline_ns-time.monotonic_ns())/1e9
            if remaining <= 0:
                raise ProbeError('command-deadline-exceeded')
            time.sleep(min(remaining, 0.05))
        check_cancel(cancellation)
        record['exitCode'] = process.returncode
        if process.returncode != 0:
            raise ProbeError('jcmd-command-failed')
    except ProbeError as error:
        code = error.code
    except subprocess.TimeoutExpired:
        code = 'command-deadline-exceeded'
    except KeyboardInterrupt:
        code = 'probe-cancelled-sigint'
    except OSError:
        code = 'jcmd-command-unavailable'
    finally:
        if process is not None:
            # This process group belongs exclusively to the diagnostic client, never the Java server.
            # The leader may have exited while descendants still own the pipes. Its returncode
            # never establishes group completion. Clean the owned group on every exit path.
            try:
                os.killpg(process.pid, signal.SIGKILL)
                record['groupCleanup'] = 'sigkill-sent'
            except ProcessLookupError:
                record['groupCleanup'] = 'already-absent'
            except OSError:
                record['groupCleanup'] = 'signal-failed'
                code = 'diagnostic-group-cleanup-failed'
            try:
                process.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                code = 'diagnostic-client-cleanup-incomplete'
            record['leaderReaped'] = process.returncode is not None
            for stream in (process.stdout, process.stderr):
                stream.close()
            record['exitCode'] = process.returncode
        selector.close()
        record['finishedMonotonicNs'] = time.monotonic_ns()
        record['errorCode'] = code
        for name, value in buffers.items():
            raw = bytes(value)
            safe = redact(raw, secret)
            (out/(stem+'.'+name+'.txt')).write_bytes(safe)
            record[name] = {'bytes': len(safe), 'sha256': sha256(safe), 'redacted': safe != raw}
        write_json(out/(stem+'.json'), record)
    if code:
        raise ProbeError(code)
    return bytes(buffers['stdout']), record


class Sentinel:
    def __init__(self, endpoint, body, authorization, deadline_ns, max_bytes, max_wire_bytes, accept_encoding):
        self.endpoint = endpoint
        self.body = body
        self.authorization = authorization
        self.deadline_ns = deadline_ns
        self.max_bytes = max_bytes
        self.max_wire_bytes = max_wire_bytes
        self.accept_encoding = accept_encoding
        self.sent = threading.Event()
        self.done = threading.Event()
        self.lock = threading.Lock()
        self.sock = None
        self.response = None
        self.raw = b''
        self.record = {}
        self.thread = threading.Thread(target=self._request, name='hgbench-mode-sentinel', daemon=True)
        self.timer = threading.Timer(max(0, (deadline_ns-time.monotonic_ns())/1e9), self.cancel)
        self.timer.daemon = True
        self.timer_started = False
        self.worker_started = False

    def remaining(self):
        remaining = (self.deadline_ns-time.monotonic_ns())/1e9
        if remaining <= 0:
            raise ProbeError('request-deadline-exceeded')
        return remaining

    def start(self):
        try:
            self.timer.start()
            self.timer_started = True
        except RuntimeError:
            raise ProbeError('sentinel-timer-start-failed') from None
        try:
            self.thread.start()
            self.worker_started = True
        except RuntimeError:
            self.timer.cancel()
            raise ProbeError('sentinel-worker-start-failed') from None

    def cancel(self):
        with self.lock:
            self.record['cancelRequested'] = True
            sock = self.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _request(self):
        connection = None
        chunks = bytearray()
        wire_bytes = 0
        wire_hash = hashlib.sha256()
        decoder = None
        try:
            self.record['startedMonotonicNs'] = time.monotonic_ns()
            host, port, path = parse_endpoint(self.endpoint)
            connection = http.client.HTTPConnection(host, port, timeout=self.remaining())
            connection.connect()
            with self.lock:
                self.sock = connection.sock
                cancelled = self.record.get('cancelRequested', False)
            if cancelled:
                raise ProbeError('request-deadline-exceeded')
            connection.sock.settimeout(self.remaining())
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                       'Accept-Encoding': self.accept_encoding, 'Connection': 'close'}
            if self.authorization:
                headers['Authorization'] = self.authorization
            connection.request('POST', path, body=self.body, headers=headers)
            self.record['sentMonotonicNs'] = time.monotonic_ns()
            self.sent.set()
            self.response = connection.getresponse()
            self.record['httpStatus'] = self.response.status
            content_type = self.response.getheader('Content-Type', '').split(';', 1)[0].strip().lower()
            encoding = self.response.getheader('Content-Encoding', 'identity').strip().lower()
            self.record['contentTypeAccepted'] = content_type == 'application/json'
            self.record['contentEncoding'] = encoding if encoding in ('identity', 'gzip') else 'unsupported'
            self.record['contentEncodingAccepted'] = (encoding == 'identity' or
                                                       (encoding == 'gzip' and self.accept_encoding == 'gzip'))
            if encoding == 'gzip':
                decoder = zlib.decompressobj(16+zlib.MAX_WBITS)
            # Python 3.12 read1() closes fp (and the final socket reference) when the
            # declared body is complete. Do not touch that closed socket on the next iteration.
            while not self.response.isclosed():
                self.sock.settimeout(self.remaining())
                chunk = self.response.read1(min(16384, self.max_wire_bytes+1-wire_bytes))
                if not chunk:
                    break
                wire_bytes += len(chunk)
                wire_hash.update(chunk)
                if wire_bytes > self.max_wire_bytes:
                    raise ProbeError('response-wire-limit-exceeded')
                chunks.extend(decoder.decompress(chunk, self.max_bytes+1-len(chunks)) if decoder else chunk)
                if len(chunks) > self.max_bytes:
                    raise ProbeError('response-body-limit-exceeded')
                if decoder and (decoder.unused_data or decoder.unconsumed_tail):
                    raise ProbeError('response-gzip-trailing-or-unconsumed-data')
            if self.response.length not in (None, 0):
                raise ProbeError('response-body-incomplete')
            if decoder and not decoder.eof:
                raise ProbeError('response-gzip-incomplete')
            self.remaining()
            self.record['completeMonotonicNs'] = time.monotonic_ns()
            self.record['errorCode'] = None
        except ProbeError as error:
            self.record['errorCode'] = error.code
        except (socket.timeout, TimeoutError):
            self.record['errorCode'] = 'request-deadline-exceeded'
        except zlib.error:
            self.record['errorCode'] = 'response-gzip-invalid'
        except (OSError, http.client.HTTPException, ValueError):
            self.record['errorCode'] = ('request-deadline-exceeded' if
                                        time.monotonic_ns() >= self.deadline_ns else 'http-request-failed')
        finally:
            self.raw = bytes(chunks[:self.max_bytes])
            self.record['bodyBytes'] = len(self.raw)
            self.record['bodySha256'] = sha256(self.raw)
            self.record['wireBytes'] = wire_bytes
            self.record['wireSha256'] = wire_hash.hexdigest()
            self.record['finishedMonotonicNs'] = time.monotonic_ns()
            if self.response is not None:
                self.response.close()
            if connection is not None:
                connection.close()
            self.timer.cancel()
            self.done.set()


def validate_response(record, raw, nonce):
    if record.get('errorCode'):
        raise ProbeError(record['errorCode'])
    if (record.get('httpStatus') != 200 or not record.get('contentTypeAccepted') or
            not record.get('contentEncodingAccepted')):
        raise ProbeError('sentinel-response-protocol-mismatch')
    try:
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('duplicate JSON key')
                result[key] = value
            return result
        result = json.loads(raw.decode('utf-8'), object_pairs_hook=unique_object,
                            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('non-JSON number')))
        if (not isinstance(result, dict) or not isinstance(result.get('status'), dict) or
                type(result['status'].get('code')) is not int or result['status']['code'] != 200 or
                not isinstance(result.get('result'), dict) or result['result'].get('data') != [nonce]):
            raise ValueError('nonce envelope differs')
    except (ValueError, UnicodeError, TypeError, RecursionError):
        raise ProbeError('sentinel-nonce-response-mismatch') from None


def verify_window(request, command):
    try:
        if not (request['sentMonotonicNs'] <= command['startedMonotonicNs'] <
                command['finishedMonotonicNs'] <= request['completeMonotonicNs']):
            raise ProbeError('thread-dump-outside-request-window')
    except KeyError:
        raise ProbeError('thread-dump-outside-request-window') from None


def validate_args(args):
    parse_endpoint(args.endpoint)
    if args.java_pid < 1 or args.java_pid == os.getpid():
        raise ProbeError('invalid-java-pid')
    bounds = [('sleep_ms', 500, 10000), ('capture_delay_ms', 0, 1000),
              ('request_timeout_seconds', 1, 60), ('command_timeout_seconds', 1, 60),
              ('total_timeout_seconds', 1, 180), ('max_command_bytes', 1024, 16*1024*1024),
              ('max_response_bytes', 128, 1024*1024), ('max_wire_response_bytes', 128, 1024*1024)]
    for name, low, high in bounds:
        value = getattr(args, name)
        if not isinstance(value, (int, float)) or not low <= value <= high:
            raise ProbeError('invalid-resource-budget')
    if (args.capture_delay_ms >= args.sleep_ms or args.request_timeout_seconds*1000 <= args.sleep_ms or
            args.total_timeout_seconds < args.request_timeout_seconds):
        raise ProbeError('inconsistent-time-budget')
    if args.authorization_env and not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', args.authorization_env):
        raise ProbeError('invalid-authorization-environment-name')
    authorization = os.environ.get(args.authorization_env) if args.authorization_env else None
    if args.authorization_env and (not authorization or len(authorization) > 8192 or
                                   any(ord(c) < 32 or ord(c) > 126 for c in authorization)):
        raise ProbeError('authorization-environment-unavailable-or-invalid')
    return authorization


def run_probe(args, cancellation=None):
    started = time.monotonic_ns()
    result = {'schemaVersion': 1, 'kind': 'hugegraph-http-mode-probe', 'status': 'mode-unverified',
              'expectedMode': args.expected_mode, 'performanceAccepted': False,
              'startedMonotonicNs': started, 'modeVerified': False,
              'startedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'outerControllerRequirements': ['exclusive-probe-window', 'immutable-build-and-config-identity',
                                              'container-identity', 'resource-limits', 'outside-measurement-window']}
    out = None
    owns_output = False
    sentinel = None
    clock_before = None
    try:
        check_cancel(cancellation)
        authorization = validate_args(args)
        deadline = started + int(args.total_timeout_seconds*1e9)
        out = Path(args.out).absolute()
        out.mkdir(mode=0o700, exist_ok=False)
        owns_output = True
        nonce = 'hgbench-mode-'+uuid.uuid4().hex
        body = json.dumps({'gremlin': 'Thread.sleep(%d); nonce' % args.sleep_ms,
                           'bindings': {'nonce': nonce}, 'language': 'gremlin-groovy'},
                          separators=(',', ':')).encode('utf-8')
        write_json(out/'request.json', {'method': 'POST', 'endpoint': args.endpoint,
                   'body': json.loads(body), 'bodySha256': sha256(body), 'bodyBytes': len(body),
                   'authorizationEnvironment': args.authorization_env, 'expectedMode': args.expected_mode,
                   'acceptEncoding': args.accept_encoding,
                   'budgets': {name: getattr(args, name) for name in ['sleep_ms', 'capture_delay_ms',
                       'request_timeout_seconds', 'command_timeout_seconds', 'total_timeout_seconds',
                       'max_command_bytes', 'max_response_bytes', 'max_wire_response_bytes']}})
        clock_before = clock_snapshot(args.java_pid, True)
        identity = inspect_process(args.java_pid, args.jcmd, args.endpoint)
        check_cancel(cancellation)
        result['identityBefore'] = identity
        command = [identity['jcmdExecutable'], str(args.java_pid)]
        version, _ = bounded_command(command+['VM.version'],
                                    min(deadline, time.monotonic_ns()+int(args.command_timeout_seconds*1e9)),
                                    args.max_command_bytes, out, 'attach', authorization, cancellation)
        if b'JDK 17.' not in version or not re.search(rb'(?m)^'+str(args.java_pid).encode()+rb':\s*$', version):
            raise ProbeError('target-jvm-version-unverified')
        if identity != inspect_process(args.java_pid, args.jcmd, args.endpoint):
            raise ProbeError('process-identity-changed')
        check_cancel(cancellation)
        request_deadline = min(deadline, time.monotonic_ns()+int(args.request_timeout_seconds*1e9))
        sentinel = Sentinel(args.endpoint, body, authorization, request_deadline, args.max_response_bytes,
                            args.max_wire_response_bytes, args.accept_encoding)
        sentinel.start()
        if not wait_event(sentinel.sent, request_deadline, cancellation):
            raise ProbeError('sentinel-not-sent')
        delay = min(args.capture_delay_ms/1000, max(0, (request_deadline-time.monotonic_ns())/1e9))
        if wait_event(sentinel.done, time.monotonic_ns()+int(delay*1e9), cancellation):
            raise ProbeError('sentinel-finished-before-stack-capture')
        stack, capture = bounded_command(command+['Thread.print'],
                                        min(deadline, request_deadline,
                                            time.monotonic_ns()+int(args.command_timeout_seconds*1e9)),
                                        args.max_command_bytes, out, 'thread-dump', authorization, cancellation)
        matched = parse_thread_dump(stack, args.expected_mode)
        if not wait_event(sentinel.done, request_deadline, cancellation):
            raise ProbeError('request-deadline-exceeded')
        validate_response(sentinel.record, sentinel.raw, nonce)
        verify_window(sentinel.record, capture)
        after = inspect_process(args.java_pid, args.jcmd, args.endpoint)
        check_cancel(cancellation)
        result['identityAfter'] = after
        if identity != after:
            raise ProbeError('process-identity-changed')
        if time.monotonic_ns() >= deadline:
            raise ProbeError('total-deadline-exceeded')
        result.update(status='passed', modeVerified=True, observedMode=matched['mode'],
                      threadEvidence={'threadHeader': redact(matched['threadHeader'].encode(),
                                                            authorization).decode(),
                                      'frames': matched['frames']})
    except ProbeError as error:
        result['errorCode'] = error.code
    except KeyboardInterrupt:
        result['errorCode'] = 'probe-cancelled-sigint'
    except (OSError, ValueError, TypeError):
        result['errorCode'] = 'probe-input-or-evidence-unavailable'
    finally:
        if sentinel is not None:
            sentinel.timer.cancel()
            if not sentinel.done.is_set():
                sentinel.cancel()
                # Socket shutdown interrupts the blocking HTTP reader; cleanup has its own fixed bound.
                if sentinel.worker_started:
                    sentinel.thread.join(timeout=0.5)
            result['request'] = dict(sentinel.record)
            result['request']['timerStarted'] = sentinel.timer_started
            result['request']['workerStarted'] = sentinel.worker_started
            if sentinel.worker_started and not sentinel.done.is_set():
                result.update(status='mode-unverified', modeVerified=False, errorCode='request-cleanup-incomplete')
        if clock_before is not None:
            clock_after = clock_snapshot(args.java_pid, False)
            changed = clock_snapshot_changes(clock_before, clock_after)
            observed = not clock_before['unavailable'] and not clock_after['unavailable'] and not changed
            result['clockIdentity'] = {'schemaVersion': 1, 'kind': 'http-jvm-mode-clock-identity',
                'sourceOrigin': clock_before['sourceOrigin'], 'before': clock_before, 'after': clock_after,
                'captureComponentSha256': clock_before['captureComponentSha256'],
                'status': 'observed' if observed else 'unknown', 'changedFields': changed,
                'verified': False, 'hostAssociation': 'unverified',
                'executableHashMaxBytes': CLOCK_EXECUTABLE_MAX_BYTES,
                'executableHashMaxNs': str(CLOCK_HASH_MAX_NS)}
        result['finishedMonotonicNs'] = time.monotonic_ns()
        if cancellation is not None and cancellation.cancelled:
            result.update(status='mode-unverified', modeVerified=False, errorCode=cancellation.code)
        result['elapsedSeconds'] = (result['finishedMonotonicNs']-started)/1e9
        if owns_output:
            try:
                result['componentSha256'] = sha256(Path(__file__).read_bytes())
                write_json(out/'result.json', result)
            except (OSError, ValueError):
                result.update(status='mode-unverified', modeVerified=False, errorCode='evidence-write-failed')
    return result


class Parser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(64, json.dumps({'status': 'mode-unverified', 'errorCode': 'invalid-arguments',
                                'nextAction': 'Run --help; invalid argument values are not echoed.'})+'\n')


def parser():
    value = Parser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                   epilog='stdout is one JSON object. Exit 0: passed; 2: mode-unverified; 64: invalid arguments.\n'
                          'No retries, redirects, proxies, privilege fallback or heap scans.\n'
                          'Existing output is refused.\n'
                          'Example (inside target container): python3 hugegraph-mode-probe.py --endpoint\n'
                          '  http://127.0.0.1:18080/gremlin --jcmd /jdk/bin/jcmd --java-pid 42\n'
                          '  --expected-mode embedded --out /evidence/probe-before-warmup\n'
                          'After a passed probe: complete the declared warmup before starting measurement.\n'
                          'After a failure: preserve this directory, diagnose, then use a new attempt directory.')
    value.add_argument('--endpoint', required=True,
                       help='Explicit http://loopback-IP:port/gremlin; no DNS/query/userinfo')
    value.add_argument('--jcmd', required=True, type=Path, help='Executable jcmd from the exact target Java 17 JDK')
    value.add_argument('--java-pid', required=True, type=int, help='Exact PID in this container; never all JVMs')
    value.add_argument('--expected-mode', choices=sorted(EXECUTORS), required=True)
    value.add_argument('--out', type=Path, required=True, help='New directory; parent must already exist')
    value.add_argument('--authorization-env',
                       help='Name of env containing full Authorization header; value never saved')
    value.add_argument('--accept-encoding', choices=['gzip', 'identity'], default='gzip',
                       help='Negotiated encoding (default: gzip); identity fallback is valid HTTP negotiation')
    value.add_argument('--sleep-ms', type=int, default=2000, help='Bounded sentinel sleep, 500..10000 (default: 2000)')
    value.add_argument('--capture-delay-ms', type=int, default=100, help='Delay after sent, 0..1000 (default: 100)')
    value.add_argument('--request-timeout-seconds', type=float, default=10, help='Absolute request limit 1..60')
    value.add_argument('--command-timeout-seconds', type=float, default=10, help='Each jcmd limit 1..60')
    value.add_argument('--total-timeout-seconds', type=float, default=30,
                       help='Total work limit 1..180; bounded cleanup may add <=0.9s')
    value.add_argument('--max-command-bytes', type=int, default=1024*1024, help='Combined stdout/stderr cap per jcmd')
    value.add_argument('--max-response-bytes', type=int, default=65536, help='Decoded HTTP body cap, 128..1048576')
    value.add_argument('--max-wire-response-bytes', type=int, default=65536, help='Wire HTTP body cap, 128..1048576')
    value.add_argument('--json', action='store_true', help='JSON is always enabled; accepted for agent consistency')
    return value


def main(argv=None):
    args = parser().parse_args(argv)
    cancellation = Cancellation()
    previous = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        for number in previous:
            signal.signal(number, cancellation.signal)
        outcome = run_probe(args, cancellation)
        print(json.dumps(outcome, ensure_ascii=True, sort_keys=True, allow_nan=False))
        return 0 if outcome['status'] == 'passed' else 2
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


if __name__ == '__main__':
    raise SystemExit(main())
