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

"""Bounded offline fixtures, not a real JDK17 or JVM-to-host clock validation."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

COMPONENT = Path(__file__).resolve().parents[1] / 'components/http/jvm-observation.py'
SPEC = importlib.util.spec_from_file_location('jvm_observation_test', COMPONENT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def line(tags, body, uptime=1000000000, pid=123):
    return ('[%dns][%dns][%d][124][debug][%s] %s\n' %
            (uptime + 9000000000000000, uptime, pid, tags, body)).encode()


class Observation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        MOD.CANCELLED = 0
        self.spec = {'schemaVersion': 1, 'kind': 'http-jvm-observation-spec', 'preset': MOD.PRESET,
                     'capture': 'supervisor-stdout', 'maxRawBytes': 2 * MOD.MiB, 'windowMs': 1000,
                     'parser': {'maxLineBytes': 4096, 'maxEvents': 10000, 'maxWindows': 100,
                                'maxOutputBytes': 8 * MOD.MiB, 'timeoutMs': 1000}}

    def freeze(self):
        MOD.write(self.root, 'input.json', MOD.encoded(self.spec))
        self.plan = self.root / 'plan'
        planned = MOD.plan(self.root / 'input.json', self.plan)
        self.launch = {'schemaVersion': 1, 'kind': 'http-jvm-observation-launch',
                       'planSha256': planned['planSha256'],
                       'argv': ['/opt/jdk/bin/java', MOD.ARGUMENT, 'fixture.Main'],
                       'process': {'pid': 123, 'startTimeTicks': '456',
                                   'hostBootId': '12345678-1234-1234-1234-123456789abc'},
                       'jdk': {'javaSha256': 'a' * 64, 'libjvmSha256': 'b' * 64, 'version': '17.0.17+10'}}
        return planned

    def capture(self, raw):
        MOD.write(self.root, 'server.log', raw)
        launch_raw = MOD.encoded(self.launch)
        MOD.write(self.root, 'launch.json', launch_raw)
        self.receipt = {'schemaVersion': 1, 'kind': 'http-jvm-observation-capture',
                        'planSha256': self.launch['planSha256'], 'launchSha256': MOD.digest(launch_raw),
                        'raw': {'bytes': len(raw), 'sha256': MOD.digest(raw)},
                        'startedMonotonicNs': '1', 'finishedMonotonicNs': '99',
                        'eof': True, 'capHit': False, 'forcedKill': False, 'cleanupComplete': True,
                        'error': None, 'readBytes': len(raw), 'retainedBytes': len(raw), 'discardedBytes': 0}

    def parse(self, receipt=None, output='parsed'):
        capture_path = self.root / (output + '-capture.json')
        MOD.write(self.root, capture_path.name, MOD.encoded(receipt or self.receipt))
        result = MOD.parse(self.plan, self.root / 'server.log', self.root / 'launch.json',
                           capture_path, self.root / output)
        manifest = json.loads((self.root / output / 'manifest.json').read_text())
        for item in manifest['files']:
            raw = (self.root / output / item['path']).read_bytes()
            self.assertEqual(item['bytes'], len(raw), item['path'])
            self.assertEqual(item['sha256'], hashlib.sha256(raw).hexdigest(), item['path'])
        self.assertLessEqual(sum(p.stat().st_size for p in (self.root / output).iterdir()),
                             self.spec['parser']['maxOutputBytes'])
        return result, json.loads((self.root / output / 'summary.json').read_text())

    def test_plan_is_explicit_frozen_and_launches_nothing(self):
        with patch.object(subprocess, 'Popen', side_effect=AssertionError('must not launch')):
            planned = self.freeze()
        self.assertFalse(planned['executed'])
        spec, _, _ = MOD.load_plan(self.plan)
        self.assertEqual(spec, self.spec)
        self.assertEqual(planned['javaArguments'], [MOD.ARGUMENT])
        with self.assertRaises(FileExistsError):
            MOD.plan(self.root / 'input.json', self.plan)
        (self.plan / 'spec.json').write_text('{}')
        with self.assertRaises(MOD.ObservationError):
            MOD.load_plan(self.plan)

    def test_gc_jit_and_safepoint_semantics_preserve_original_clocks(self):
        self.freeze()
        raw = (b'ordinary application line\n' + line('gc', 'Using G1') +
               line('gc,start', 'GC(0) Pause Young (Normal) (G1 Evacuation Pause)', 1100000000) +
               line('gc', 'GC(0) Pause Young (Normal) (G1 Evacuation Pause) 64M->8M(128M) 1.125ms',
                    1101125000) + line('jit,compilation', '  12 %     4       fixture.Main::work @ 3 (18 bytes)') +
               line('jit,compilation', '  12 %     4       fixture.Main::work @ 3 (18 bytes)   made not entrant') +
               line('jit,compilation', '  12 %     4       fixture.Main::work @ 3 (18 bytes)   made zombie') +
               line('safepoint', 'Safepoint "Cleanup", Time since last: 900 ns, Reaching safepoint: 10 ns, '
                    'Cleanup: 20 ns, At safepoint: 30 ns, Total: 60 ns', 2000000000))
        self.capture(raw)
        result, summary = self.parse()
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(summary['gcPauseReportedNs'], '1125000')
        self.assertEqual(summary['safepointReportedNs'], '60')
        self.assertEqual(summary['counts']['jit-begin'], 1)
        self.assertEqual(summary['counts']['jit-not-entrant'], 1)
        self.assertEqual(summary['counts']['jit-zombie'], 1)
        self.assertEqual(summary['applicationLines'], 1)
        self.assertEqual(summary['clockAlignment'], 'unverified')
        self.assertIsNone(summary['warmupStable'])
        self.assertIsNone(summary['compilationSuccesses'])
        self.assertIsNone(summary['compilationCpuNs'])
        events = [json.loads(s) for s in (self.root / 'parsed/events.ndjson').read_text().splitlines()]
        self.assertEqual(events[3]['level'], 'debug')
        self.assertEqual(events[3]['compilationLevel'], '4')
        self.assertEqual(events[2]['heapApproxMiB'], {'before': 64, 'after': 8, 'capacity': 128})
        self.assertTrue(events[2]['startObserved'])
        self.assertEqual(int(events[0]['timenanos']) - int(events[0]['uptimenanos']), 9000000000000000)
        for event in events:
            source = event['source']
            self.assertTrue(raw[source['offset']:source['offset'] + source['bytes']].startswith(b'['))

    def test_capture_missing_eof_cap_or_forced_kill_preserves_partial(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        for key, value in [('eof', False), ('capHit', True), ('forcedKill', True), ('cleanupComplete', False)]:
            result, summary = self.parse(dict(self.receipt, **{key: value}), key)
            self.assertEqual(result['status'], 'partial')
            self.assertFalse(summary['captureComplete'])
            self.assertTrue(summary['rawComplete'])

    def test_actual_jdk17_native_wrappers_and_padded_tid_preserve_raw(self):
        self.freeze()
        self.launch['process']['pid'] = 219
        # Exact individual lines read from v4 arm 32389d... environment/work/server.log,
        # at original offsets 3062, 3570, and 1400439. This concatenation is a parser
        # fixture, with its own offsets, not a replacement capture or v4 acceptance.
        raw = (b'[135459660303558ns][73967348ns][219][220][debug][jit,compilation] '
               b'  12     n 0       java.lang.Object::getClass (native)   \n'
               b'[135459661837937ns][75501593ns][219][220][debug][jit,compilation] '
               b'  16     n 0       java.lang.System::arraycopy (native)   (static)\n'
               b'[135471824892205ns][12238555852ns][219][233 ][debug][jit,compilation] '
               b'6569       1       org.apache.hugegraph.type.HugeType::string (5 bytes)\n')
        self.capture(raw)
        result, summary = self.parse()
        self.assertEqual(result['status'], 'completed', result)
        self.assertTrue(summary['rawComplete'])
        self.assertTrue(summary['captureComplete'])
        self.assertEqual(summary['counts'], {'jit-native-wrapper': 2, 'jit-begin': 1})
        self.assertIsNone(summary['compilationSuccesses'])
        self.assertIsNone(summary['compilationCpuNs'])
        self.assertFalse(summary['performanceAccepted'])
        self.assertEqual((self.root / 'parsed/server.log').read_bytes(), raw)
        events = [json.loads(s) for s in (self.root / 'parsed/events.ndjson').read_text().splitlines()]
        self.assertEqual(events[2]['tid'], 233)
        offset = 0
        for event, original in zip(events, raw.splitlines(keepends=True)):
            self.assertEqual(event['source'], {'offset': offset, 'bytes': len(original)})
            self.assertEqual(raw[offset:offset + len(original)], original)
            offset += len(original)

    def test_numeric_decorators_allow_only_bounded_ascii_right_padding(self):
        original = line('gc', 'Using G1')
        padded = original.replace(b'ns]', b'ns ]').replace(b'[124]', b'[124    ]')
        event = MOD.decode_event(padded, 0, 123)
        self.assertEqual(event['tid'], 124)
        self.assertEqual(event['timenanos'], '9000001000000000')
        invalid = [original.replace(b'[124]', value) for value in
                   (b'[ 124]', b'[124\t]', b'[12 4]', b'[00124]', b'[0]', b'[4194305]', b'[124     ]')]
        invalid += [original.replace(b'ns]', b'ns      ]', 1),
                    original.replace(b'[123]', b'[999 ]'),
                    original.replace(b'[123]', b'[123     ]')]
        for index, raw in enumerate(invalid):
            with self.subTest(raw=raw):
                old = self.root
                self.root = old / str(index)
                self.root.mkdir()
                try:
                    self.freeze()
                    self.capture(raw + original)
                    result, summary = self.parse()
                    self.assertEqual(result['status'], 'partial')
                    self.assertFalse(summary['rawComplete'])
                    self.assertEqual(summary['events'], 0)
                finally:
                    self.root = old

    def test_source_shaped_native_wrapper_variants_remain_separate_activity(self):
        for flags, level in [('    n', '0 '), (' s  n', '0 '), ('    n', '')]:
            body = '  12 %s %s     fixture.Native::call (native)   ' % (flags, level)
            with self.subTest(body=body):
                event = MOD.decode_event(line('jit,compilation', body), 0, 123)
                self.assertEqual(event['kind'], 'jit-native-wrapper')

    def test_tampered_or_unknown_native_wrapper_syntax_stays_partial(self):
        self.freeze()
        base = '  12     n 0       fixture.Native::call (native)   '
        bodies = [base[:-3], base[:-1], base + '(STATIC)', base + '(static) extra',
                  base.replace('    n', '     '), base.replace('n 0', 'n 4'),
                  base.replace('    n', 'n    '), base.replace('n 0', 'n\t0')]
        self.capture(b''.join(line('jit,compilation', body) for body in bodies))
        result, summary = self.parse()
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(summary['counts'], {'unknown-jit': len(bodies)})
        self.assertEqual(result['issues'], ['unknown-jit'])
        self.assertTrue(summary['rawComplete'])
        self.assertNotIn('jit-begin', summary['counts'])
        self.assertIsNone(summary['compilationSuccesses'])
        self.assertIsNone(summary['compilationCpuNs'])

    def test_eof_after_io_loss_or_short_write_cannot_be_complete(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        changed = dict(self.receipt, error='fixture short write', readBytes=self.receipt['readBytes'] + 99,
                       discardedBytes=99)
        result, summary = self.parse(changed)
        self.assertIn('capture-io-error', result['issues'])
        self.assertIn('capture-discarded-bytes', result['issues'])
        self.assertFalse(summary['captureComplete'])
        self.assertTrue(summary['rawComplete'])
        inconsistent = dict(self.receipt, discardedBytes=99)
        with self.assertRaisesRegex(MOD.ObservationError, 'capture-byte-accounting-differs'):
            self.parse(inconsistent, 'inconsistent')

    def test_identity_digest_and_handwritten_alignment_cannot_pass(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        bad = dict(self.receipt, alignment='passed')
        with self.assertRaisesRegex(MOD.ObservationError, 'capture-fields'):
            self.parse(bad)
        self.assertFalse((self.root / 'parsed').exists())
        bad = dict(self.receipt, raw={'bytes': self.receipt['raw']['bytes'], 'sha256': '0' * 64})
        result, summary = self.parse(bad, output='digest')
        self.assertIn('capture-raw-digest-differs', result['issues'])
        self.assertFalse(summary['rawComplete'])

    def test_wrong_pid_and_malformed_prefix_fail_closed(self):
        for index, raw in enumerate((line('gc', 'Using G1', pid=999), b'[1ns][bad] malformed\n',
                                     line('gc', 'Using G1') + b'[0.001s][info][gc] GC(0) hidden pause\n')):
            with self.subTest(index=index):
                root = self.root / str(index)
                root.mkdir()
                old = self.root
                self.root = root
                try:
                    self.freeze()
                    self.capture(raw)
                    result, _ = self.parse()
                    self.assertEqual(result['status'], 'partial')
                finally:
                    self.root = old

    def test_unknown_metric_records_and_unmatched_gc_stay_visible(self):
        self.freeze()
        self.capture(line('jit,compilation', 'new unknown syntax') +
                     line('gc', 'GC(9) Pause Full (Fixture) 64M->8M(128M) 1.000ms'))
        result, summary = self.parse()
        self.assertIn('unknown-jit', result['issues'])
        self.assertIn('unmatched-or-reversed-gc-pause', result['issues'])
        self.assertEqual(summary['events'], 2)

    def test_truncated_or_oversized_line_remains_partial_and_bounded(self):
        self.freeze()
        self.capture(line('gc', 'Using G1')[:-1])
        result, _ = self.parse()
        self.assertIn('truncated-final-line', result['issues'])
        (self.root / 'server.log').write_bytes(b'x' * (self.spec['parser']['maxLineBytes'] + 10000))
        result, _ = self.parse(output='long')
        self.assertIn('line-byte-budget', result['issues'])
        self.assertEqual((self.root / 'long/server.log').stat().st_size, self.spec['parser']['maxLineBytes'] + 1)

    def test_zero_events_do_not_prove_jit_stability(self):
        self.freeze()
        self.capture(b'application only\n')
        result, summary = self.parse()
        self.assertIn('no-unified-log-events', result['issues'])
        self.assertIsNone(summary['warmupStable'])

    def test_events_and_windows_have_independent_caps(self):
        self.spec['parser'].update(maxEvents=1, maxWindows=1)
        self.freeze()
        self.capture(line('gc', 'Using G1') + line('gc', 'Using G1', 2000000000))
        result, summary = self.parse()
        self.assertIn('event-count-budget', result['issues'])
        self.assertEqual(summary['events'], 1)
        self.spec['parser']['maxEvents'] = 100
        other = self.root / 'other'
        other.mkdir()
        self.root = other
        self.freeze()
        self.capture(line('gc', 'Using G1') + line('gc', 'Using G1', 2000000000))
        result, _ = self.parse()
        self.assertIn('window-count-budget', result['issues'])

    def test_deadline_and_cancel_save_partial_manifest(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        with patch.object(MOD.time, 'monotonic', side_effect=[1, 100, 100]):
            result, _ = self.parse()
        self.assertIn('parse-deadline', result['issues'])
        MOD.CANCELLED = 143
        result, _ = self.parse(output='cancel')
        self.assertIn('cancelled', result['issues'])

    def test_output_budget_bounds_expansion_and_preserves_sealed_prefix(self):
        self.spec['parser'].update(maxEvents=20000, maxOutputBytes=4 * MOD.MiB, timeoutMs=5000)
        self.freeze()
        self.capture(line('gc', 'Using G1') * 15000)
        result, summary = self.parse()
        self.assertIn('output-byte-budget', result['issues'])
        self.assertGreater(summary['events'], 0)
        self.assertLess(summary['events'], 15000)

    def test_real_sigterm_during_parse_seals_partial_with_fixed_exit(self):
        self.freeze()
        self.capture(line('gc', 'Using G1') * 10)
        MOD.write(self.root, 'capture.json', MOD.encoded(self.receipt))
        code = '''import importlib.util,os,signal,sys
from pathlib import Path
s=importlib.util.spec_from_file_location('probe',sys.argv[1]); m=importlib.util.module_from_spec(s)
s.loader.exec_module(m); signal.signal(signal.SIGTERM,m.cancel)
original=m.decode_event
def interrupt(raw,offset,pid):
    if offset==0: os.kill(os.getpid(),signal.SIGTERM)
    return original(raw,offset,pid)
m.decode_event=interrupt
r=Path(sys.argv[2]); m.parse(r/'plan',r/'server.log',r/'launch.json',r/'capture.json',r/'signal')
sys.exit(m.CANCELLED)
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, COMPONENT, self.root],
                                capture_output=True, text=True, timeout=3)
        self.assertEqual(result.returncode, 143, result.stderr)
        summary = json.loads((self.root / 'signal/summary.json').read_text())
        self.assertEqual(summary['status'], 'partial')
        self.assertIn('cancelled', summary['issues'])
        manifest = json.loads((self.root / 'signal/manifest.json').read_text())
        for item in manifest['files']:
            self.assertEqual(MOD.digest((self.root / 'signal' / item['path']).read_bytes()), item['sha256'])

    def test_raw_size_fifo_and_symlinks_do_not_block(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        raw = self.root / 'server.log'
        with raw.open('wb') as stream:
            stream.truncate(self.spec['maxRawBytes'] + 1)
        result, _ = self.parse()
        self.assertIn('raw-byte-budget', result['issues'])
        raw.unlink()
        os.mkfifo(raw)
        begun = time.monotonic()
        result, _ = self.parse(output='fifo')
        self.assertLess(time.monotonic() - begun, 1)
        self.assertEqual(result['status'], 'partial')
        raw.unlink()
        raw.symlink_to(self.root / 'launch.json')
        result, _ = self.parse(output='link')
        self.assertEqual(result['status'], 'partial')

    def test_changing_source_is_not_complete_capture(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        original_decode = MOD.decode_event
        changed = [False]
        def mutate(*args):
            if not changed[0]:
                with (self.root / 'server.log').open('ab') as stream:
                    stream.write(b'changed\n')
                changed[0] = True
            return original_decode(*args)
        with patch.object(MOD, 'decode_event', side_effect=mutate):
            result, _ = self.parse()
        self.assertIn('raw-input-changed', result['issues'])

    def test_component_mutation_keeps_original_identity_and_invalidates_result(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        copy = self.root / 'component-copy.py'
        copy.write_bytes(COMPONENT.read_bytes())
        original = MOD.decode_event
        def change_component(*args):
            with copy.open('ab') as stream:
                stream.write(b'\n# changed while parsing\n')
            return original(*args)
        with patch.object(MOD, '__file__', str(copy)), patch.object(MOD, 'decode_event', change_component):
            result, summary = self.parse()
        self.assertIn('component-changed', result['issues'])
        self.assertFalse(summary['componentReverified'])
        manifest = json.loads((self.root / 'parsed/manifest.json').read_text())
        plan = json.loads((self.plan / 'plan.json').read_text())
        self.assertEqual(manifest['componentSha256'], plan['componentSha256'])
        self.assertNotEqual(manifest['componentSha256'], MOD.digest(copy.read_bytes()))

    def test_cancel_during_final_component_check_is_not_completed(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        original, calls = MOD.read, [0]
        def late_cancel(path, *args):
            raw = original(path, *args)
            if Path(path) == Path(MOD.__file__):
                calls[0] += 1
                if calls[0] == 2:
                    MOD.CANCELLED = 143
            return raw
        with patch.object(MOD, 'read', late_cancel):
            result, _ = self.parse()
        self.assertIn('cancelled', result['issues'])
        self.assertEqual(result['status'], 'partial')

    def test_schema_rejects_unknown_fields_and_async_argv(self):
        bad = dict(self.spec, aligned=True)
        with self.assertRaisesRegex(MOD.ObservationError, 'spec-fields'):
            MOD.validate_spec(bad)
        self.freeze()
        self.launch['argv'].append('-Xlog:async')
        self.capture(line('gc', 'Using G1'))
        with self.assertRaisesRegex(MOD.ObservationError, 'launch-logging-options-differ'):
            self.parse()

    def test_cli_help_and_structured_failure(self):
        result = subprocess.run([sys.executable, '-B', COMPONENT, '--help'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn('offline', result.stdout)
        result = subprocess.run([sys.executable, '-B', COMPONENT, '--json', 'describe'],
                                capture_output=True, text=True)
        self.assertFalse(json.loads(result.stdout)['data']['liveOperations'])
        result = subprocess.run([sys.executable, '-B', COMPONENT, 'plan', '--spec', self.root / 'missing',
                                 '--out', self.root / 'new'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 3)
        self.assertFalse(json.loads(result.stdout)['error']['retryable'])

    def test_public_cli_parse_distinguishes_complete_and_partial(self):
        self.freeze()
        self.capture(line('gc', 'Using G1'))
        for eof, code in [(True, 0), (False, 2)]:
            receipt = self.root / ('capture-' + str(code) + '.json')
            MOD.write(self.root, receipt.name, MOD.encoded(dict(self.receipt, eof=eof)))
            result = subprocess.run([sys.executable, '-B', COMPONENT, '--json', 'parse',
                        '--plan', self.plan, '--raw', self.root / 'server.log',
                        '--launch', self.root / 'launch.json', '--capture', receipt,
                        '--out', self.root / ('report-' + str(code))], capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, code, result.stdout + result.stderr)
            data = json.loads(result.stdout)
            self.assertEqual(data['ok'], eof)
            self.assertEqual(data['data']['status'], 'completed' if eof else 'partial')


if __name__ == '__main__':
    unittest.main()
