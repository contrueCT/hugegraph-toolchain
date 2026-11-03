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

"""Mode probe parser and bounded local fixture checks; no real HugeGraph/JVM claim."""
import copy
import gzip
import http.server
import importlib.util
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('http_mode_probe', ROOT/'components/http/hugegraph-mode-probe.py')
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def thread(name, frames):
    return ('"'+name+'" #1 prio=5 os_prio=0 tid=0x00000001 nid=0x02 waiting on condition\n'
            '   java.lang.Thread.State: TIMED_WAITING (parking)\n'+
            ''.join('    at '+frame+'(Fixture.java:123)\n' for frame in frames)+'\n')


def dump(mode='embedded', extra=''):
    frames = ['java.util.concurrent.CompletableFuture.get']
    if mode == 'embedded':
        frames.append(probe.PREFIX+'EmbeddedGremlinRestExecutor.execute')
    frames.extend([probe.PREFIX+probe.EXECUTORS[mode]+'.post', probe.PREFIX+'GremlinAPI.post'])
    return ('12345:\n2026-10-22\nFull thread dump OpenJDK 64-Bit Server VM (17 mixed mode):\n\n'+
            thread('rest-worker', frames)+extra+'JNI global refs: 12, weak refs: 0\n').encode()


class ThreadDumpTest(unittest.TestCase):
    def test_final_read1_can_close_socket_before_next_iteration(self):
        nonce = 'closed-socket-final-chunk'
        body = json.dumps({'status': {'code': 200}, 'result': {'data': [nonce]}}).encode()
        wire = gzip.compress(body)

        class Socket:
            closed = False

            def settimeout(self, value):
                if self.closed:
                    raise OSError(9, 'Bad file descriptor')

        sock = Socket()

        class Response:
            status = 200
            length = len(wire)
            closed = False

            def getheader(self, name, default=None):
                return {'Content-Type': 'application/json', 'Content-Encoding': 'gzip'}.get(name, default)

            def read1(self, limit):
                self.length = 0
                self.closed = True
                sock.closed = True
                return wire

            def isclosed(self):
                return self.closed

            def close(self):
                self.closed = True

        response = Response()

        class Connection:
            def __init__(self, *args, **kwargs):
                self.sock = sock

            def connect(self):
                pass

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                return response

            def close(self):
                pass

        request = probe.Sentinel('http://127.0.0.1:18080/gremlin', b'{}', None,
                                 time.monotonic_ns()+int(1e9), 65536, 65536, 'gzip')
        with patch.object(probe.http.client, 'HTTPConnection', Connection):
            request._request()
        self.assertIsNone(request.record['errorCode'], request.record)
        self.assertEqual(request.record['wireBytes'], len(wire))
        self.assertEqual(request.raw, body)
        probe.validate_response(request.record, request.raw, nonce)

    def test_expected_modes_match_one_real_call_chain(self):
        for mode in probe.EXECUTORS:
            result = probe.parse_thread_dump(dump(mode), mode)
            self.assertEqual(result['mode'], mode)
            self.assertIn('rest-worker', result['threadHeader'])
            named = dump(mode).replace(b'at org.apache.', b'at app//org.apache.')
            self.assertEqual(probe.parse_thread_dump(named, mode)['mode'], mode)

    def test_wrong_mode_fails(self):
        with self.assertRaisesRegex(probe.ProbeError, 'actual-mode-mismatch'):
            probe.parse_thread_dump(dump('forward'), 'embedded')

    def test_api_and_executor_in_different_threads_never_combine(self):
        value = (b'Full thread dump fixture:\n'+thread('rest', [probe.PREFIX+'GremlinAPI.post']).encode()+
                 thread('other', [probe.PREFIX+'EmbeddedGremlinRestExecutor.post']).encode()+
                 b'JNI global refs: 0\n')
        with self.assertRaises(probe.ProbeError):
            probe.parse_thread_dump(value, 'embedded')

    def test_name_mentions_without_stack_frames_do_not_count(self):
        value = dump().replace(b'    at '+probe.PREFIX.encode(), b'   comment '+probe.PREFIX.encode())
        with self.assertRaisesRegex(probe.ProbeError, 'expected-one-active'):
            probe.parse_thread_dump(value, 'embedded')

    def test_mixed_modes_in_same_thread_fail(self):
        value = dump().replace(b'    at java.util', b'    at '+probe.PREFIX.encode()+
                               b'ForwardGremlinRestExecutor.post(F.java:1)\n    at java.util')
        with self.assertRaisesRegex(probe.ProbeError, 'ambiguous-rest-thread'):
            probe.parse_thread_dump(value, 'embedded')

    def test_other_active_executor_or_duplicate_rest_thread_fails(self):
        for extra in [thread('other', [probe.PREFIX+'ForwardGremlinRestExecutor.post']),
                      thread('other', [probe.PREFIX+'EmbeddedGremlinRestExecutor.post',
                                       probe.PREFIX+'GremlinAPI.post'])]:
            with self.assertRaises(probe.ProbeError):
                probe.parse_thread_dump(dump(extra=extra), 'embedded')

    def test_wrong_call_order_fails(self):
        value = (b'Full thread dump fixture:\n'+thread('rest', [probe.PREFIX+'GremlinAPI.post',
                  probe.PREFIX+'EmbeddedGremlinRestExecutor.post']).encode()+b'JNI global refs: 0\n')
        with self.assertRaisesRegex(probe.ProbeError, 'call-order-invalid'):
            probe.parse_thread_dump(value, 'embedded')

    def test_truncated_invalid_utf8_and_invalid_headers_fail(self):
        for value in [dump().split(b'JNI global refs:')[0], dump()+b'\xff',
                      dump().replace(b'tid=0x00000001', b'no-thread-id')]:
            with self.assertRaises(probe.ProbeError):
                probe.parse_thread_dump(value, 'embedded')

    def test_window_must_be_wholly_inside_inflight_request(self):
        request = {'sentMonotonicNs': 10, 'completeMonotonicNs': 30}
        probe.verify_window(request, {'startedMonotonicNs': 11, 'finishedMonotonicNs': 29})
        for start, end in [(9, 20), (20, 31), (20, 20), (30, 20)]:
            with self.assertRaises(probe.ProbeError):
                probe.verify_window(request, {'startedMonotonicNs': start, 'finishedMonotonicNs': end})
        with self.assertRaises(probe.ProbeError):
            probe.verify_window({}, {'startedMonotonicNs': 11, 'finishedMonotonicNs': 20})

    def test_exact_nonce_json_and_protocol(self):
        record = {'httpStatus': 200, 'contentTypeAccepted': True, 'contentEncodingAccepted': True}
        body = {'status': {'code': 200}, 'result': {'data': ['nonce']}}
        probe.validate_response(record, json.dumps(body).encode(), 'nonce')
        bad = [b'not-json', json.dumps({**body, 'result': {'data': ['other']}}).encode(),
               json.dumps({**body, 'result': {'data': ['nonce', 'extra']}}).encode(),
               b'{"status":{"code":200},"status":{"code":200},"result":{"data":["nonce"]}}',
               b'{"status":{"code":200},"result":{"data":["nonce"]},"extra":NaN}']
        for raw in bad:
            with self.assertRaisesRegex(probe.ProbeError, 'nonce-response-mismatch'):
                probe.validate_response(record, raw, 'nonce')
        for changed in [{'httpStatus': 302}, {'contentTypeAccepted': False}, {'contentEncodingAccepted': False},
                        {'errorCode': 'request-deadline-exceeded'}]:
            with self.assertRaises(probe.ProbeError):
                probe.validate_response({**record, **changed}, json.dumps(body).encode(), 'nonce')


class IdentityParsingTest(unittest.TestCase):
    def test_process_identity_checks_jdk_namespace_and_owned_listener(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proc = root/'proc'
            java_pid = proc/'12345'
            for path in [root/'jdk/bin', java_pid/'ns', java_pid/'fd', java_pid/'net',
                         proc/'self/ns', proc/'sys/kernel/random']:
                path.mkdir(parents=True, exist_ok=True)
            java = root/'jdk/bin/java'
            java.write_bytes(b'fixture Java executable; not executed')
            jcmd = root/'jdk/bin/jcmd'
            jcmd.write_bytes(b'fixture jcmd executable; not executed')
            jcmd.chmod(0o700)
            (java_pid/'exe').symlink_to(java)
            stat = '12345 (java (fixture)) '+' '.join(['S']+['0']*18+['4321']+['0']*5)
            (java_pid/'stat').write_text(stat)
            (java_pid/'status').write_text('Uid: 0 %d 0 0\nGid: 0 %d 0 0\n' % (os.geteuid(), os.getegid()))
            for name in ['pid', 'net', 'mnt', 'user']:
                (java_pid/'ns'/name).symlink_to(name+':[123]')
                (proc/'self/ns'/name).symlink_to(name+':[123]')
            (java_pid/'fd/7').symlink_to('socket:[456]')
            (java_pid/'net/tcp').write_text('header\n'
                                           '0: 0100007F:46A0 00000000:0000 0A 0:0 0:0 0 1000 0 456\n')
            (java_pid/'cgroup').write_text('0::/docker/'+('a'*64)+'\n')
            (proc/'sys/kernel/random/boot_id').write_text('fixture-boot-id\n')
            identity = probe.inspect_process(12345, jcmd, 'http://127.0.0.1:18080/gremlin', proc)
            self.assertEqual(identity['listenerSocketInodes'], ['456'])
            self.assertEqual(identity['startTimeTicks'], '4321')
            self.assertEqual(identity['containerIdHints'], ['a'*64])
            (root/'other-jdk/bin').mkdir(parents=True)
            (root/'other-jdk/bin/java').write_bytes(b'other Java')
            other = root/'other-jdk/bin/jcmd'
            other.write_bytes(b'other jcmd')
            other.chmod(0o700)
            with self.assertRaisesRegex(probe.ProbeError, 'target-jdk'):
                probe.inspect_process(12345, other, 'http://127.0.0.1:18080/gremlin', proc)
            with self.assertRaisesRegex(probe.ProbeError, 'listener-not-owned'):
                probe.inspect_process(12345, jcmd, 'http://127.0.0.1:18081/gremlin', proc)
            (java_pid/'fd/7').unlink()
            with self.assertRaisesRegex(probe.ProbeError, 'listener-not-owned'):
                probe.inspect_process(12345, jcmd, 'http://127.0.0.1:18080/gremlin', proc)
            (java_pid/'fd/7').symlink_to('socket:[456]')
            (java_pid/'ns/net').unlink()
            (java_pid/'ns/net').symlink_to('net:[different]')
            with self.assertRaisesRegex(probe.ProbeError, 'namespace-mismatch'):
                probe.inspect_process(12345, jcmd, 'http://127.0.0.1:18080/gremlin', proc)
            (java_pid/'ns/net').unlink()
            (java_pid/'ns/net').symlink_to('net:[123]')
            (java_pid/'status').write_text('Uid: 0 %d 0 0\nGid: 0 %d 0 0\n' %
                                           (os.geteuid()+1, os.getegid()))
            with self.assertRaisesRegex(probe.ProbeError, 'effective-user-group-mismatch'):
                probe.inspect_process(12345, jcmd, 'http://127.0.0.1:18080/gremlin', proc)

    def test_endpoint_requires_literal_loopback_and_exact_path(self):
        self.assertEqual(probe.parse_endpoint('http://127.0.0.1:18080/gremlin'), ('127.0.0.1', 18080, '/gremlin'))
        self.assertEqual(probe.parse_endpoint('http://[::1]:18080/gremlin'), ('::1', 18080, '/gremlin'))
        for endpoint in ['http://localhost:18080/gremlin', 'http://192.0.2.1:80/gremlin',
                         'https://127.0.0.1:80/gremlin', 'http://u:secret@127.0.0.1:80/gremlin',
                         'http://127.0.0.1:80/gremlin?secret=yes', 'http://127.0.0.1:80/gremlin#frag',
                         'http://127.0.0.1:80/other', 'http://127.0.0.1/gremlin']:
            with self.assertRaises(probe.ProbeError):
                probe.parse_endpoint(endpoint)

    def test_proc_stat_handles_comm_spaces_and_parentheses(self):
        values = ['S']+['0']*18+['54321']+['0']*5
        self.assertEqual(probe.proc_stat('2 (java (worker)) '+' '.join(values)), '54321')
        with self.assertRaises(probe.ProbeError):
            probe.proc_stat('2 (java) '+' '.join(['Z']+values[1:]))

    def test_listener_requires_address_port_and_listen_state(self):
        header = 'sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n'
        row = '0: 0100007F:46A0 00000000:0000 0A 0:0 0:0 0 1000 0 12345\n'
        self.assertEqual(probe.listener_inodes(header+row, socket.AF_INET, '127.0.0.1', 18080), {'12345'})
        self.assertFalse(probe.listener_inodes(header+row, socket.AF_INET, '127.0.0.2', 18080))
        self.assertFalse(probe.listener_inodes(header+row, socket.AF_INET, '127.0.0.1', 18081))
        self.assertFalse(probe.listener_inodes(header+row.replace(' 0A ', ' 01 '), socket.AF_INET,
                                               '127.0.0.1', 18080))
        mapped = row.replace('0100007F', '0000000000000000FFFF00000100007F')
        self.assertEqual(probe.listener_inodes(header+mapped, socket.AF_INET6, '127.0.0.1', 18080), {'12345'})


class ClockIdentityTest(unittest.TestCase):
    def fixture_snapshot(self, digest=True, offsets='monotonic 0 0\nboottime 0 0\n'):
        def raw(path, cap):
            path = str(path)
            if path.endswith('/stat'):
                pid = os.getpid() if '/self/' in path else 12345
                return (str(pid)+' (fixture) '+' '.join(['S']+['0']*18+['42'])+'\n').encode()
            if path.endswith('/boot_id'):
                return b'12345678-1234-1234-1234-123456789abc\n'
            if path.endswith('/timens_offsets'):
                return offsets.encode()
            raise AssertionError(path)

        def executable(path, hashed):
            return {'sourcePath': str(path), 'path': '/fixture/python' if '/self/' in str(path) else '/fixture/java',
                    'metadata': {'bytes': '100', 'deviceMajor': 8, 'deviceMinor': 1, 'inode': '12',
                                 'mtimeNs': '100', 'ctimeNs': '200'},
                    'sha256': 'a'*64 if hashed else None, 'digestRead': hashed}
        info = SimpleNamespace(implementation='clock_gettime(CLOCK_MONOTONIC)', monotonic=True,
                               adjustable=False, resolution=1e-9)
        with patch.object(probe.sys, 'platform', 'linux'), patch.object(probe.CLOCK, 'bounded_file', side_effect=raw), \
                patch.object(probe.CLOCK, 'clock_executable', side_effect=executable), \
                patch.object(probe.os, 'readlink', return_value='time:[123]'), \
                patch.object(probe.time, 'get_clock_info', return_value=info):
            return probe.clock_snapshot(12345, digest)

    def test_all_raw_reads_are_enclosed_and_only_first_python_binary_is_hashed(self):
        before, after = self.fixture_snapshot(), self.fixture_snapshot(False)
        self.assertFalse(before['unavailable'])
        self.assertLessEqual(int(before['startedMonotonicNs']), int(before['finishedMonotonicNs']))
        self.assertLessEqual(int(before['finishedMonotonicNs']), int(after['startedMonotonicNs']))
        self.assertEqual(before['capture']['executable']['sha256'], 'a'*64)
        self.assertIsNone(after['capture']['executable']['sha256'])
        self.assertIsNone(before['jvm']['executable']['sha256'])
        self.assertEqual(probe.clock_snapshot_changes(before, after), [])
        after['capture']['statRaw'] = after['capture']['statRaw'].replace(' S ', ' R ')
        after['jvm']['statAfterRaw'] += ' '
        self.assertEqual(probe.clock_snapshot_changes(before, after), [])

    def test_drift_is_extracted_from_stable_fields_separately_from_raw_stat(self):
        before, after = self.fixture_snapshot(), self.fixture_snapshot(False)
        after['jvm']['startTimeTicksAfter'] = '43'
        after['capture']['executable']['metadata']['ctimeNs'] = '201'
        after['jvm']['timeNamespace'] = 'time:[124]'
        self.assertEqual(probe.clock_snapshot_changes(before, after),
                         ['capture.executable.metadata', 'jvm.process', 'jvm.timeNamespace'])

    def test_malformed_offsets_are_explicit_unknown(self):
        value = self.fixture_snapshot(offsets='monotonic 0 1000000000\nboottime 0 0\n')
        self.assertEqual(value['unavailable']['capture.timeOffsetsRaw'], 'clock-time-offsets-malformed')
        self.assertIsNone(value['capture']['timeOffsetsRaw'])

    def test_permission_failure_is_explicit_and_snapshot_still_finishes(self):
        real = probe.CLOCK.bounded_file
        def read(path, cap):
            if str(path).endswith('/timens_offsets'):
                raise PermissionError('fixture')
            return real(path, cap)
        with patch.object(probe.sys, 'platform', 'linux'), patch.object(probe.CLOCK, 'bounded_file', side_effect=read):
            value = probe.clock_snapshot(12345, False)
        self.assertIn('capture.timeOffsetsRaw', value['unavailable'])
        self.assertIsNone(value['capture']['timeOffsetsRaw'])
        self.assertLessEqual(int(value['startedMonotonicNs']), int(value['finishedMonotonicNs']))

    def test_fixture_never_reads_host_proc_or_claims_production_origin(self):
        with patch.object(probe.CLOCK, 'bounded_file', side_effect=AssertionError('host read')):
            value = probe.clock_snapshot(12345, True, Path('/fixture/proc'))
        self.assertEqual(value['sourceOrigin'], 'fixture')
        self.assertIsNone(value['capture'])
        self.assertEqual(value['unavailable'], {'source': 'fixture-has-no-production-clock-identity'})
        self.assertIsNone(value['captureComponentSha256'])

    def test_fixed_helper_source_hash_is_bounded_and_failure_is_raw_unknown(self):
        actual = probe.CLOCK.clock_component_sha256()
        self.assertEqual(actual, probe.sha256(Path(probe.CLOCK.__file__).read_bytes()))
        with tempfile.TemporaryDirectory() as directory:
            large = Path(directory)/'helper.py'
            large.write_bytes(b'x'*(1024*1024+1))
            with patch.object(probe.CLOCK, '__file__', str(large)):
                with self.assertRaisesRegex(probe.CLOCK.ClockCaptureError, 'component-byte-budget'):
                    probe.CLOCK.clock_component_sha256()
        with patch.object(probe.CLOCK, 'clock_component_sha256', side_effect=PermissionError('fixture')):
            value = self.fixture_snapshot()
        self.assertIsNone(value['captureComponentSha256'])
        self.assertEqual(value['unavailable']['component'], 'read-unavailable-or-malformed')

    def test_actual_open_descriptor_hash_is_bounded_and_metadata_drift_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, link = Path(directory)/'binary', Path(directory)/'exe'
            binary.write_bytes(b'fixture unexecuted executable')
            link.symlink_to(binary)
            value = probe.clock_executable(link, True)
            self.assertEqual(value['sha256'], probe.sha256(binary.read_bytes()))
            self.assertEqual(value['metadata']['bytes'], str(binary.stat().st_size))
            with patch.object(probe.CLOCK, 'CLOCK_EXECUTABLE_MAX_BYTES', 2):
                with self.assertRaisesRegex(probe.CLOCK.ClockCaptureError, 'byte-budget'):
                    probe.clock_executable(link, True)
            with patch.object(probe.time, 'monotonic_ns', side_effect=[0, probe.CLOCK_HASH_MAX_NS]):
                with self.assertRaisesRegex(probe.CLOCK.ClockCaptureError, 'hash-deadline'):
                    probe.clock_executable(link, True)
            with patch.object(probe.time, 'monotonic_ns', side_effect=[0, 1, 2, 3, probe.CLOCK_HASH_MAX_NS]):
                with self.assertRaisesRegex(probe.CLOCK.ClockCaptureError, 'hash-deadline'):
                    probe.clock_executable(link, True)
            first = probe.clock_file_metadata(binary.stat())
            with patch.object(probe.CLOCK, 'clock_file_metadata', side_effect=[first, first, {**first, 'ctimeNs': '0'}]):
                with self.assertRaisesRegex(probe.CLOCK.ClockCaptureError, 'changed-during-read'):
                    probe.clock_executable(link, False)


class LocalFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='hgbench-mode-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.jcmd = self.root/'jcmd'
        self.jcmd.write_text('#!/bin/sh\nprintf "%s:\\n" "$1"\n'
                             'if [ "$2" = VM.version ]; then printf "JDK 17.0.19 fixture\\n"; '
                             'else printf "%s" '+shlex.quote(dump().decode())+'; fi\n')
        self.jcmd.chmod(0o700)
        self.behavior = {'nonce': 'correct', 'delay': 0.65}
        self.auth_seen = None
        self.capture_complete = threading.Event()
        real_command = probe.bounded_command

        def command(*args, **kwargs):
            result = real_command(*args, **kwargs)
            if args[0][-1] == 'Thread.print':
                self.capture_complete.set()
            return result
        command_patch = patch.object(probe, 'bounded_command', side_effect=command)
        command_patch.start()
        self.addCleanup(command_patch.stop)
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                outer.auth_seen = self.headers.get('Authorization')
                if outer.behavior.get('slowHeaders'):
                    try:
                        for byte in b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n':
                            self.wfile.write(bytes([byte]))
                            self.wfile.flush()
                            time.sleep(0.1)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
                time.sleep(outer.behavior['delay'])
                # Successful fixtures coordinate their response with actual subprocess completion.
                # Fixed wall-clock races are tested separately, not assumed in every happy-path test.
                if not outer.behavior.get('early'):
                    outer.capture_complete.wait(timeout=12)
                nonce = body['bindings']['nonce'] if outer.behavior['nonce'] == 'correct' else 'wrong'
                data = json.dumps({'status': {'code': 200}, 'result': {'data': [nonce]}}).encode()
                if outer.behavior.get('large'):
                    data += b' '*4096
                encoding = outer.behavior.get('encoding', 'gzip')
                if encoding == 'gzip':
                    data = gzip.compress(data)
                    if outer.behavior.get('gzipIncomplete'):
                        data = data[:-4]
                    if outer.behavior.get('gzipCorrupt'):
                        data = data[:-5]+bytes([data[-5] ^ 0xff])+data[-4:]
                    if outer.behavior.get('gzipExtraMember'):
                        data += gzip.compress(b'extra')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Encoding', encoding)
                self.send_header('Content-Length', str(len(data)+(9 if outer.behavior.get('truncate') else 0)))
                self.send_header('Connection', 'close')
                self.end_headers()
                try:
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.args = probe.parser().parse_args([
            '--endpoint', 'http://127.0.0.1:%d/gremlin' % self.server.server_port,
            '--jcmd', str(self.jcmd), '--java-pid', '12345', '--expected-mode', 'embedded',
            '--out', str(self.root/'attempt'), '--sleep-ms', '500', '--capture-delay-ms', '20'])
        self.identity = {'pid': 12345, 'startTimeTicks': '100', 'jcmdExecutable': str(self.jcmd),
                         'testFixture': True, 'listenerSocketInodes': ['123']}
        self.identity_calls = 0
        self.change_identity = False

        def identity(*args):
            self.identity_calls += 1
            result = copy.deepcopy(self.identity)
            if self.change_identity and self.identity_calls > 2:
                result['startTimeTicks'] = '999'
            return result
        self.identity_patch = patch.object(probe, 'inspect_process', side_effect=identity)
        self.identity_patch.start()
        self.addCleanup(self.identity_patch.stop)

    def run_probe(self):
        self.capture_complete.clear()
        value = probe.run_probe(self.args)
        self.assertEqual(value, json.loads((self.args.out/'result.json').read_text()))
        return value

    def test_local_fixture_runs_request_subprocess_and_evidence(self):
        result = self.run_probe()
        self.assertEqual(result['status'], 'passed', result)
        self.assertTrue(result['modeVerified'])
        self.assertFalse(result['performanceAccepted'])
        self.assertEqual(result['request']['contentEncoding'], 'gzip')
        self.assertTrue(result['identityBefore']['testFixture'])
        self.assertTrue((self.args.out/'thread-dump.stdout.txt').is_file())
        self.assertEqual(self.identity_calls, 3)

    def test_clock_read_failure_is_preserved_without_changing_mode_proof(self):
        before = ClockIdentityTest().fixture_snapshot(offsets='invalid')
        after = copy.deepcopy(before)
        with patch.object(probe, 'clock_snapshot', side_effect=[before, after]):
            result = self.run_probe()
        self.assertTrue(result['modeVerified'], result)
        self.assertEqual(result['clockIdentity']['status'], 'unknown')
        self.assertEqual(result['clockIdentity']['before']['unavailable']['jvm.timeOffsetsRaw'],
                         'clock-time-offsets-malformed')

    def test_wrong_nonce_fails_with_preserved_stack(self):
        self.behavior['nonce'] = 'wrong'
        result = self.run_probe()
        self.assertEqual(result['errorCode'], 'sentinel-nonce-response-mismatch')
        self.assertFalse(result['modeVerified'])
        self.assertTrue((self.args.out/'thread-dump.stdout.txt').exists())

    def test_response_limit_and_truncated_content_length(self):
        for behavior, reason in [({'large': True}, 'response-body-limit-exceeded'),
                                 ({'truncate': True}, 'response-body-incomplete')]:
            self.args.out = self.root/reason
            self.behavior.update({'large': False, 'truncate': False, **behavior})
            self.args.max_response_bytes = 256
            self.assertEqual(self.run_probe()['errorCode'], reason)

    def test_absolute_request_deadline_is_fail_closed(self):
        self.behavior['delay'] = 3
        self.args.request_timeout_seconds = 1
        started = time.monotonic()
        result = self.run_probe()
        self.assertEqual(result['status'], 'mode-unverified')
        self.assertIn(result['errorCode'], ['request-deadline-exceeded', 'request-cleanup-incomplete'])
        self.assertLess(time.monotonic()-started, self.args.total_timeout_seconds+0.9)
        request = result['request']
        self.assertLessEqual((request['finishedMonotonicNs']-request['startedMonotonicNs'])/1e9, 1.9)

    def test_gzip_crc_eof_extra_members_and_wire_limit_fail_closed(self):
        cases = [({'gzipIncomplete': True}, 'response-gzip-incomplete'),
                 ({'gzipCorrupt': True}, 'response-gzip-invalid'),
                 ({'gzipExtraMember': True}, 'response-gzip-trailing-or-unconsumed-data'),
                 ({'encoding': 'identity', 'large': True}, 'response-wire-limit-exceeded')]
        for behavior, reason in cases:
            self.args.out = self.root/reason
            self.behavior = {'nonce': 'correct', 'delay': 0.65, **behavior}
            self.args.max_wire_response_bytes = 128 if reason == 'response-wire-limit-exceeded' else 65536
            self.assertEqual(self.run_probe()['errorCode'], reason)

    def test_explicit_identity_rejects_unsolicited_gzip(self):
        self.args.accept_encoding = 'identity'
        self.assertEqual(self.run_probe()['errorCode'], 'sentinel-response-protocol-mismatch')
        self.args.out = self.root/'identity'
        self.behavior['encoding'] = 'identity'
        result = self.run_probe()
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['request']['contentEncoding'], 'identity')

    def test_process_restart_invalidates_mode(self):
        self.change_identity = True
        result = self.run_probe()
        self.assertEqual(result.get('errorCode'), 'process-identity-changed', result)

    def test_slow_header_trickle_cannot_extend_absolute_request_deadline(self):
        self.behavior['slowHeaders'] = True
        self.args.request_timeout_seconds = 1
        started = time.monotonic()
        result = self.run_probe()
        self.assertEqual(result['status'], 'mode-unverified')
        self.assertIn(result['errorCode'], ['request-deadline-exceeded', 'request-cleanup-incomplete'])
        self.assertLess(time.monotonic()-started, self.args.total_timeout_seconds+0.9)
        request = result['request']
        self.assertLessEqual((request['finishedMonotonicNs']-request['startedMonotonicNs'])/1e9, 1.9)

    def test_existing_attempt_is_never_written(self):
        self.args.out.mkdir()
        (self.args.out/'marker').write_text('keep')
        result = probe.run_probe(self.args)
        self.assertEqual(result['status'], 'mode-unverified')
        self.assertEqual([p.name for p in self.args.out.iterdir()], ['marker'])

    def test_credentials_only_referenced_and_not_propagated_to_jcmd(self):
        secret = 'Bearer SUPER-PRIVATE-MODE-TEST-TOKEN'
        self.args.authorization_env = 'HGBENCH_PROBE_AUTH'
        self.jcmd.write_text('#!/bin/sh\n'
                             '[ -z "${HGBENCH_PROBE_AUTH+x}" ] || exit 1\n'
                             '[ -z "${JAVA_TOOL_OPTIONS+x}" ] || exit 1\n'
                             'printf "%s:\\n" "$1"\n'
                             'if [ "$2" = VM.version ]; then printf "JDK 17.0.19 fixture\\n"; '
                             'else printf "%s" '+shlex.quote(dump().decode())+'; fi\n')
        with patch.dict(os.environ, {'HGBENCH_PROBE_AUTH': secret, 'JAVA_TOOL_OPTIONS': secret}):
            result = self.run_probe()
        self.assertEqual(result['status'], 'passed', result)
        self.assertEqual(self.auth_seen, secret)
        for entry in self.args.out.iterdir():
            self.assertNotIn(secret.encode(), entry.read_bytes())
        self.assertNotIn(secret, json.dumps(result))

    def test_wrong_mode_stack_and_early_response_are_fail_closed(self):
        self.args.expected_mode = 'forward'
        self.assertEqual(self.run_probe()['errorCode'], 'actual-mode-mismatch')
        self.args.out = self.root/'early'
        self.args.expected_mode = 'embedded'
        self.behavior['delay'] = 0
        self.behavior['early'] = True
        self.args.capture_delay_ms = 100
        self.assertEqual(self.run_probe()['errorCode'], 'sentinel-finished-before-stack-capture')

    def test_timer_and_worker_start_failures_preserve_result(self):
        original_start = threading.Thread.start
        for fail_timer, reason in [(True, 'sentinel-timer-start-failed'), (False, 'sentinel-worker-start-failed')]:
            self.args.out = self.root/reason

            def start(thread):
                if ((fail_timer and isinstance(thread, threading.Timer)) or
                        (not fail_timer and thread.name == 'hgbench-mode-sentinel')):
                    raise RuntimeError('cannot start new thread')
                return original_start(thread)

            with patch.object(threading.Thread, 'start', start):
                result = self.run_probe()
            self.assertEqual(result['errorCode'], reason)
            self.assertEqual(result['status'], 'mode-unverified')
            self.assertFalse(result['request']['workerStarted'])
            self.assertEqual(result['request']['timerStarted'], not fail_timer)

    def test_keyboard_interrupt_still_cancels_inflight_request_and_writes_result(self):
        real_command = probe.bounded_command

        def interrupt(*args, **kwargs):
            if args[0][-1] == 'Thread.print':
                raise KeyboardInterrupt()
            return real_command(*args, **kwargs)

        with patch.object(probe, 'bounded_command', side_effect=interrupt):
            result = self.run_probe()
        self.assertEqual(result['errorCode'], 'probe-cancelled-sigint')
        self.assertEqual(result['status'], 'mode-unverified')
        self.assertTrue(result['request']['cancelRequested'])


class BoundedCommandTest(unittest.TestCase):
    def test_signal_handler_does_not_take_event_condition_lock(self):
        # Reproduce the old deadlock in an isolated process with a hard caller timeout.
        # Even if a legacy caller holds Event's non-reentrant condition, the handler
        # only assigns plain fields and must return for both the first and next signal.
        program = ('import importlib.util,signal,threading,os\n'
                   's=importlib.util.spec_from_file_location("p", '+
                   repr(str(ROOT/'components/http/hugegraph-mode-probe.py'))+')\n'
                   'p=importlib.util.module_from_spec(s)\ns.loader.exec_module(p)\n'
                   'c=p.Cancellation()\nc.event=threading.Event()\n'
                   'signal.signal(signal.SIGTERM,c.signal)\n'
                   'c.event._cond.acquire()\n'
                   'os.kill(os.getpid(),signal.SIGTERM)\n'
                   'os.kill(os.getpid(),signal.SIGTERM)\n'
                   'assert c.cancelled and c.code == "probe-cancelled-sigterm"\n'
                   'c.event._cond.release()\nprint("handler-returned")\n')
        result = subprocess.run([sys.executable, '-c', program], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), 'handler-returned')

    def fork_fixture(self, out):
        child_pid = out/'child.pid'
        fixture = out/'fork.py'
        fixture.write_text('import os,time\nfrom pathlib import Path\n'
                           'child=os.fork()\n'
                           'if child == 0:\n    time.sleep(30)\n    os._exit(0)\n'
                           'Path('+repr(str(child_pid))+').write_text(str(child))\n'
                           'os._exit(0)\n')
        return fixture, child_pid

    def assert_child_stopped(self, path):
        self.assertTrue(path.is_file(), 'fork fixture never started; lifecycle path was not exercised')
        pid = int(path.read_text())
        deadline = time.monotonic()+3
        try:
            while time.monotonic() < deadline:
                if sys.platform.startswith('linux'):
                    try:
                        raw = (Path('/proc')/str(pid)/'stat').read_text()
                        state = raw.rsplit(')', 1)[1].strip().split()[0]
                    except FileNotFoundError:
                        state = ''
                else:
                    state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)],
                                           capture_output=True, text=True, timeout=2).stdout.strip()
                # A killed orphan can briefly remain a zombie until the OS's adopter reaps it.
                if not state or state[0] in ('Z', 'X', 'x'):
                    return
                time.sleep(0.02)
            self.fail('diagnostic descendant remained runnable after cleanup')
        finally:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    @unittest.skipUnless(hasattr(os, 'fork'), 'requires POSIX fork/process groups')
    def test_exited_leader_descendant_inheriting_stdout_is_killed_on_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            fixture, child_pid = self.fork_fixture(out)
            with self.assertRaisesRegex(probe.ProbeError, 'command-deadline-exceeded'):
                probe.bounded_command([sys.executable, str(fixture)],
                                      time.monotonic_ns()+int(2e9), 1024, out, 'fork', None)
            self.assert_child_stopped(child_pid)
            record = json.loads((out/'fork.json').read_text())
            self.assertEqual(record['exitCode'], 0)
            self.assertEqual(record['groupCleanup'], 'sigkill-sent')
            self.assertTrue(record['leaderReaped'])

    @unittest.skipUnless(hasattr(os, 'fork'), 'requires POSIX fork/process groups')
    def test_keyboard_interrupt_kills_group_after_leader_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            fixture, child_pid = self.fork_fixture(out)
            factory = probe.selectors.DefaultSelector

            class InterruptSelector:
                def __init__(self):
                    self.inner = factory()

                def __getattr__(self, name):
                    return getattr(self.inner, name)

                def select(self, timeout):
                    if child_pid.exists():
                        raise KeyboardInterrupt()
                    return self.inner.select(timeout)

            with patch.object(probe.selectors, 'DefaultSelector', InterruptSelector):
                with self.assertRaisesRegex(probe.ProbeError, 'probe-cancelled-sigint'):
                    probe.bounded_command([sys.executable, str(fixture)],
                                          time.monotonic_ns()+int(5e9), 1024, out, 'interrupt', None)
            self.assert_child_stopped(child_pid)
            record = json.loads((out/'interrupt.json').read_text())
            self.assertEqual(record['errorCode'], 'probe-cancelled-sigint')
            self.assertEqual(record['groupCleanup'], 'sigkill-sent')

    @unittest.skipUnless(hasattr(os, 'fork'), 'requires POSIX fork/process groups')
    def test_cli_sigterm_and_sigint_write_partial_result_and_kill_descendants(self):
        for signum, reason in [(signal.SIGTERM, 'probe-cancelled-sigterm'),
                               (signal.SIGINT, 'probe-cancelled-sigint')]:
            with tempfile.TemporaryDirectory() as directory:
                out = Path(directory)
                fixture, child_pid = self.fork_fixture(out)
                jcmd = out/'jcmd'
                jcmd.write_text('#!/bin/sh\nexec '+shlex.quote(sys.executable)+' '+
                                 shlex.quote(str(fixture))+'\n')
                jcmd.chmod(0o700)
                args = ['--endpoint', 'http://127.0.0.1:18080/gremlin', '--java-pid', '12345',
                        '--jcmd', str(jcmd), '--expected-mode', 'embedded', '--out', str(out/'attempt')]
                launch = out/'launch.py'
                # This fixture replaces only /proc identity; actual CLI, handlers, fork and cleanup run.
                launch.write_text('import importlib.util\n'
                                  's=importlib.util.spec_from_file_location("p", '+
                                  repr(str(ROOT/'components/http/hugegraph-mode-probe.py'))+')\n'
                                  'p=importlib.util.module_from_spec(s)\ns.loader.exec_module(p)\n'
                                  'p.inspect_process=lambda *a: '+repr({'jcmdExecutable': str(jcmd),
                                                                        'pid': 12345, 'testFixture': True})+'\n'
                                  'raise SystemExit(p.main('+repr(args)+'))\n')
                process = subprocess.Popen([sys.executable, str(launch)],
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    deadline = time.monotonic()+5
                    while not child_pid.exists() and time.monotonic() < deadline:
                        if process.poll() is not None:
                            break
                        time.sleep(0.02)
                    self.assertTrue(child_pid.exists(), 'CLI never entered diagnostic command')
                    process.send_signal(signum)
                    process.send_signal(signum)
                    stdout, stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 2, stderr)
                    result = json.loads(stdout)
                    self.assertEqual(result['status'], 'mode-unverified')
                    self.assertEqual(result['errorCode'], reason)
                    self.assertEqual(json.loads((out/'attempt/result.json').read_text()), result)
                    self.assertEqual(json.loads((out/'attempt/attach.json').read_text())['errorCode'], reason)
                    self.assert_child_stopped(child_pid)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=3)
                    if child_pid.exists():
                        try:
                            os.kill(int(child_pid.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_timeout_and_output_overflow_preserve_bounded_evidence(self):
        for code, limit, duration, reason in [
            ('import time;time.sleep(5)', 1024, 0.1, 'command-deadline-exceeded'),
            ('print("x"*10000)', 1024, 1, 'command-output-limit-exceeded')]:
            with tempfile.TemporaryDirectory() as directory:
                out = Path(directory)
                start = time.monotonic()
                with self.assertRaisesRegex(probe.ProbeError, reason):
                    probe.bounded_command([sys.executable, '-c', code],
                                          time.monotonic_ns()+int(duration*1e9), limit, out, 'test', None)
                self.assertLess(time.monotonic()-start, 2)
                self.assertLessEqual(sum((out/('test.'+name+'.txt')).stat().st_size
                                         for name in ['stdout', 'stderr']), limit)
                self.assertEqual(json.loads((out/'test.json').read_text())['errorCode'], reason)

    def test_diagnostic_output_redacts_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            probe.bounded_command([sys.executable, '-c', 'print("SECRET-TEST")'],
                                  time.monotonic_ns()+int(1e9), 1024, out, 'test', 'SECRET-TEST')
            self.assertEqual((out/'test.stdout.txt').read_text(), '*'*len('SECRET-TEST')+'\n')

    def test_cli_help_and_invalid_arguments_are_machine_friendly(self):
        help_result = subprocess.run([sys.executable, str(ROOT/'components/http/hugegraph-mode-probe.py'), '--help'],
                                     capture_output=True, text=True, timeout=5)
        self.assertEqual(help_result.returncode, 0)
        self.assertIn('never inside a measurement window', help_result.stdout)
        invalid = subprocess.run([sys.executable, str(ROOT/'components/http/hugegraph-mode-probe.py'),
                                  '--secret=NEVER-ECHO-THIS'], capture_output=True, text=True, timeout=5)
        self.assertEqual(invalid.returncode, 64)
        self.assertEqual(json.loads(invalid.stderr)['errorCode'], 'invalid-arguments')
        self.assertNotIn('NEVER-ECHO-THIS', invalid.stdout+invalid.stderr)


if __name__ == '__main__':
    unittest.main()
