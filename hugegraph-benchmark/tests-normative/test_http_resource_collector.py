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

"""Deterministic procfs/cgroup fixtures and actual CLI cancellation; no Linux/Docker performance claim."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / 'components/http/resource-collector.py'
SPEC = importlib.util.spec_from_file_location('http_resources', MODULE)
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


def fixture(root):
    proc, cg = root / 'proc', root / 'cgroup'
    group = cg / 'target'
    psi = 'some avg10=0.00 avg60=0.00 avg300=0.00 total=100\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'
    files = {proc / 'self/mountinfo': f'100 99 0:1 / {cg} rw - cgroup2 cgroup rw\n',
             proc / 'sys/kernel/random/boot_id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n',
             proc / '123/stat': '123 (fixture (worker)) ' + ' '.join(['S'] + ['0'] * 18 + ['4321'] + ['0'] * 5),
             proc / '123/cgroup': '0::/target\n', proc / 'stat': 'cpu 100 0 50 1000 1 0 0 0 0 0\n',
             proc / 'meminfo': ''.join(f'{key}: 1024 kB\n' for key in C.MEM_FIELDS),
             proc / 'loadavg': '0.10 0.20 0.30 1/100 123\n',
             proc / 'diskstats': '8 0 sda 1 0 8 1 2 0 16 2 0 3 4\n',
             group / 'cgroup.controllers': 'cpu memory pids io\n', group / 'cgroup.type': 'domain\n',
             group / 'cgroup.events': 'populated 1\nfrozen 0\n',
             group / 'cpu.stat': 'usage_usec 10000\nuser_usec 8000\nsystem_usec 2000\nnr_throttled 0\n',
             group / 'cpu.max': '200000 100000\n', group / 'memory.current': '1048576\n',
             group / 'memory.peak': '2097152\n', group / 'memory.max': '1073741824\n',
             group / 'memory.swap.current': '0\n', group / 'memory.swap.max': '0\n',
             group / 'memory.stat': 'anon 100\nfile 200\n', group / 'memory.events': 'oom 0\noom_kill 0\n',
             group / 'pids.current': '4\n', group / 'pids.max': '512\n', group / 'pids.events': 'max 0\n',
             group / 'io.stat': '8:0 rbytes=4096 wbytes=8192 rios=1 wios=2\n'}
    for name in ('cpu', 'memory', 'io'):
        files[proc / 'pressure' / name] = psi
        files[group / (name + '.pressure')] = psi
    for path, value in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    (proc / 'self/ns').mkdir()
    (proc / 'self/ns/pid').symlink_to('pid:[12345]')
    spec = {'schemaVersion': 1, 'kind': 'http-resource-collection', 'intervalMs': 100, 'durationMs': 240,
            'maxSamples': 10, 'maxOutputBytes': 1048576, 'filesystemPath': str(root),
            'roles': [{'role': 'target', 'hostPid': 123}]}
    return proc, group, spec


class Resources(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='hgbench-resource-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.proc, self.group, self.spec = fixture(self.root)
        self.out = self.root / 'evidence'

    def run_collection(self, kernel=None):
        result = C.collect(self.spec, self.out, kernel=kernel or C.Kernel(self.proc, fixture=True))
        self.assertEqual(result, json.loads((self.out / 'summary.json').read_text()))
        manifest = json.loads((self.out / 'manifest.json').read_text())
        for entry in manifest['files']:
            raw = (self.out / entry['path']).read_bytes()
            self.assertEqual(entry['bytes'], len(raw))
            self.assertEqual(entry['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertLessEqual(sum(p.stat().st_size for p in self.out.iterdir()), self.spec['maxOutputBytes'])
        return result, [json.loads(line) for line in (self.out / 'samples.ndjson').read_text().splitlines()]

    def test_complete_fixture_records_identity_raw_counters_host_and_overhead(self):
        result, rows = self.run_collection()
        self.assertEqual(result['status'], 'completed')
        self.assertGreaterEqual(result['samples'], 2)
        self.assertEqual(result['sourceOrigin'], 'fixture')
        self.assertFalse(result['performanceAccepted'])
        self.assertFalse(result['overheadCalibrated'])
        self.assertFalse(result['jvmMetrics']['available'])
        self.assertTrue(result['identityContinuous'])
        self.assertEqual(rows[0]['roles'][0]['identity']['startTimeTicks'], '4321')
        self.assertEqual(rows[0]['roles'][0]['values']['cpu.max']['quotaMicros'], '200000')
        self.assertEqual(rows[0]['host']['memoryBytes']['MemAvailable'], '1048576')
        self.assertIsNone(rows[0]['roles'][0]['interval'])
        self.assertEqual(rows[1]['roles'][0]['interval']['averageCpuCores'], 0)
        self.assertEqual(rows[1]['roles'][0]['interval']['counters']['memory.events']['oom_kill'], '0')
        self.assertLess(int(rows[0]['startedMonotonicNs']), int(rows[0]['finishedMonotonicNs']))
        target = result['observedResources']['target']
        self.assertEqual(target['maximumObservedMemoryCurrentBytes'], '1048576')
        self.assertEqual(target['maximumObservedPids'], '4')
        self.assertEqual(target['cpuUsageMicros'], '0')
        self.assertGreater(target['cpuObservedIntervals'], 0)
        self.assertEqual(target['oomKillsWithinObservedIntervals'], '0')
        clock = result['clockIdentity']
        self.assertEqual(clock['sourceOrigin'], 'fixture')
        self.assertEqual(clock['availability'], 'unknown')
        self.assertFalse(clock['verified'])
        self.assertEqual(clock['hostAssociation'], 'unverified')
        self.assertEqual(clock['changedFields'], [])
        self.assertIsNone(clock['before']['executable'])
        self.assertIsNone(clock['after']['executable'])
        self.assertLessEqual(int(clock['before']['finishedMonotonicNs']), int(result['startedMonotonicNs']))
        self.assertGreaterEqual(int(clock['after']['startedMonotonicNs']),
                                int(result['lastSampleFinishedMonotonicNs']))

    def test_fixture_clock_identity_never_reads_the_actual_host(self):
        with mock.patch.object(C, 'read_file', side_effect=AssertionError('unexpected real proc read')), \
                mock.patch.object(C, 'clock_executable_identity', side_effect=AssertionError('unexpected hash')):
            result = C.collector_clock_identity('fixture', True)
        self.assertEqual(result['availability'], 'unknown')
        self.assertEqual(result['unavailable']['source'], 'fixture-has-no-production-clock-identity')
        self.assertIsNone(result['pid'])
        self.assertIsNone(result['clock'])

    def test_clock_executable_hash_is_bounded_and_after_capture_does_not_rehash(self):
        file = self.root / 'fixture-executable'
        file.write_bytes(b'fixture-binary')
        before, error = C.clock_executable_identity(file, True)
        self.assertIsNone(error)
        self.assertEqual(before['sha256'], hashlib.sha256(file.read_bytes()).hexdigest())
        self.assertEqual(before['metadata']['bytes'], str(file.stat().st_size))
        with mock.patch.object(C.hashlib, 'sha256', side_effect=AssertionError('ending capture rehashed')):
            after, error = C.clock_executable_identity(file, False)
        self.assertIsNone(error)
        self.assertEqual(after['metadata'], before['metadata'])
        self.assertIsNone(after['sha256'])
        self.assertFalse(after['digestRead'])
        with mock.patch.object(C, 'CLOCK_EXECUTABLE_MAX_BYTES', 4):
            rejected, error = C.clock_executable_identity(file, True)
        self.assertEqual(error, 'clock-executable-byte-budget')
        self.assertEqual(rejected['metadata'], before['metadata'])
        self.assertIsNone(rejected['sha256'])
        with mock.patch.object(C, 'CLOCK_HASH_MAX_NS', 2), \
                mock.patch.object(C.time, 'monotonic_ns', side_effect=[0, 1, 3]):
            rejected, error = C.clock_executable_identity(file, True)
        self.assertEqual(error, 'clock-executable-hash-deadline')
        self.assertIsNone(rejected['sha256'])
        fifo = self.root / 'clock-fifo'
        os.mkfifo(fifo)
        started = time.monotonic()
        rejected, error = C.clock_executable_identity(fifo, True)
        self.assertEqual(error, 'not-regular-file')
        self.assertLess(time.monotonic() - started, 0.2)

    def test_collector_clock_binary_drift_or_read_loss_seals_partial_with_original_samples(self):
        base_capture = C.collector_clock_identity
        for loss in (False, True):
            with self.subTest(loss=loss):
                root = self.root / ('loss' if loss else 'changed')
                root.mkdir()
                self.proc, self.group, self.spec = fixture(root)
                self.out = root / 'evidence'
                file = root / 'fixture-executable'
                file.write_bytes(b'initial')

                def capture(origin, hash_executable):
                    result = base_capture(origin, hash_executable)
                    result['executable'], error = C.clock_executable_identity(file, hash_executable)
                    if error:
                        result['unavailable']['executable'] = error
                    return result

                def change():
                    file.unlink() if loss else file.write_bytes(b'changed-binary')

                with mock.patch.object(C, 'collector_clock_identity', side_effect=capture):
                    result, rows = self.run_collection(self.changing_kernel(change))
                self.assertEqual((result['status'], result['reason']),
                                 ('partial', 'collector-clock-identity-changed'))
                self.assertFalse(result['identityContinuous'])
                self.assertGreaterEqual(result['samples'], 2)
                self.assertEqual(len(rows), result['samples'])
                self.assertEqual(result['clockIdentity']['changedFields'], ['executable.metadata'])
                self.assertEqual(result['clockIdentity']['sourceOrigin'], 'fixture')
                self.assertFalse(result['clockIdentity']['verified'])
                self.assertTrue(result['sampleFileComplete'])

    def test_collector_clock_pid_namespace_offsets_and_clock_drift_are_partial(self):
        base_capture = C.collector_clock_identity
        patches = [('pid', 123, 124), ('startTimeTicks', '123', '124'),
                   ('bootId', 'before', 'after'), ('timeNamespace', 'time:[1]', 'time:[2]'),
                   ('timeForChildrenNamespace', 'time:[1]', 'time:[2]'),
                   ('timeOffsets', {'monotonic': '0'}, {'monotonic': '1'}),
                   ('clock', {'implementation': 'before'}, {'implementation': 'after'})]
        for field, old, new in patches:
            with self.subTest(field=field):
                root = self.root / field
                root.mkdir()
                self.proc, self.group, self.spec = fixture(root)
                self.out = root / 'evidence'

                def capture(origin, hash_executable):
                    result = base_capture(origin, hash_executable)
                    result[field] = old if hash_executable else new
                    return result

                with mock.patch.object(C, 'collector_clock_identity', side_effect=capture):
                    result, _ = self.run_collection()
                self.assertEqual(result['status'], 'partial')
                self.assertEqual(result['clockIdentity']['changedFields'], [field])
                self.assertEqual(result['clockIdentity']['availability'], 'unknown')

    def test_known_capture_drift_fails_before_the_first_sample(self):
        base_capture = C.collector_clock_identity

        def capture(origin, hash_executable):
            result = base_capture(origin, hash_executable)
            if hash_executable:
                result['captureDrift'] = ['executable']
            return result

        with mock.patch.object(C, 'collector_clock_identity', side_effect=capture):
            result, rows = self.run_collection()
        self.assertEqual((result['status'], result['reason']), ('partial', 'collector-clock-identity-changed'))
        self.assertEqual(result['samples'], 0)
        self.assertEqual(rows, [])
        self.assertIn('executable', result['clockIdentity']['changedFields'])

    def test_explicit_path_requires_matching_stable_anchor(self):
        self.spec['roles'] = [{'role': 'target', 'cgroupPath': str(self.group), 'anchorPid': 123}]
        result, _ = self.run_collection()
        self.assertEqual(result['status'], 'completed')
        self.spec['roles'][0]['cgroupPath'] = str(self.root)
        self.out = self.root / 'wrong-path'
        result, _ = self.run_collection()
        self.assertEqual(result['reason'], 'anchor-cgroup-path-mismatch')
        self.assertEqual(result['samples'], 0)

    def test_missing_and_malformed_metrics_are_null_and_never_zero(self):
        (self.group / 'memory.peak').unlink()
        (self.group / 'cpu.stat').unlink()
        (self.group / 'pids.current').write_text('NaN\n')
        (self.proc / 'pressure/io').unlink()
        result, rows = self.run_collection()
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['coverage'], 'partial')
        values = rows[0]['roles'][0]
        self.assertIsNone(values['values']['memory.peak'])
        self.assertIsNone(values['values']['pids.current'])
        self.assertEqual(values['unavailable']['memory.peak'], 'missing')
        self.assertEqual(values['unavailable']['pids.current'], 'malformed')
        self.assertIn('target.memory.peak', result['missingMetrics'])
        self.assertIsNone(rows[0]['host']['pressure']['io'])
        self.assertIsNone(result['observedResources']['target']['cpuUsageMicros'])
        self.assertIsNone(result['observedResources']['target']['averageCpuCoresOverObservedIntervals'])

    def changing_kernel(self, change):
        class Changing(C.Kernel):
            calls = 0

            def host(inner, filesystem):
                inner.calls += 1
                if inner.calls == 2:
                    change()
                return super().host(filesystem)
        return Changing(self.proc, fixture=True)

    def test_pid_restart_cgroup_replacement_migration_and_exit_stop_collection(self):
        for change in ('pid-restart', 'directory-replaced', 'process-exit', 'migration'):
            with self.subTest(change=change):
                root = self.root / change
                root.mkdir()
                proc, group, spec = fixture(root)
                self.proc, self.group, self.spec, self.out = proc, group, spec, root / 'evidence'

                def mutate():
                    if change == 'pid-restart':
                        path = proc / '123/stat'
                        path.write_text(path.read_text().replace('4321', '4322'))
                    elif change == 'directory-replaced':
                        group.rename(root / 'original-group')
                        shutil.copytree(root / 'original-group', group)
                    elif change == 'process-exit':
                        (proc / '123/stat').unlink()
                    else:
                        shutil.copytree(group, group.parent / 'other')
                        (proc / '123/cgroup').write_text('0::/other\n')
                result, rows = self.run_collection(self.changing_kernel(mutate))
                self.assertEqual(result['status'], 'partial')
                self.assertEqual(result['samples'], 1)
                self.assertFalse(result['identityContinuous'])
                self.assertTrue(result['sealed'])
                self.assertTrue(rows)

    def test_reset_and_controls_changes_preserve_rejected_observation(self):
        for file, content, reason in [('cpu.stat', 'usage_usec 1\n', 'cgroup-counter-reset'),
                                      ('cpu.max', '100000 100000\n', 'cgroup-controls-changed')]:
            with self.subTest(file=file):
                root = self.root / file
                root.mkdir()
                self.proc, self.group, self.spec = fixture(root)
                self.out = root / 'evidence'
                kernel = self.changing_kernel(lambda: (self.group / file).write_text(content))
                result, rows = self.run_collection(kernel)
                self.assertEqual(result['reason'], reason)
                self.assertTrue(result['rejectedObservationPreserved'])
                self.assertEqual(rows[-1]['kind'], 'http-resource-rejected-sample')
                self.assertEqual(rows[-1]['rejection'], reason)
                self.assertEqual(result['samples'], 1)

    def test_output_and_sample_budgets_preserve_bounded_sealed_partial(self):
        self.spec['durationMs'] = 800
        self.spec['maxOutputBytes'] = 131072
        (self.group / 'memory.stat').write_text(''.join(f'counter_{index} {index}\n' for index in range(1800)))
        result, _ = self.run_collection()
        self.assertEqual(result['reason'], 'output-byte-budget')
        self.assertEqual(result['status'], 'partial')
        self.spec['maxSamples'] = 1
        self.spec['maxOutputBytes'] = 1048576
        self.out = self.root / 'one-sample'
        result, _ = self.run_collection()
        self.assertEqual(result['reason'], 'sample-count-budget')
        self.assertEqual(result['samples'], 1)

    def test_slow_capture_records_skips_without_catchup_burst(self):
        class Slow(C.Kernel):
            def group(inner, identity):
                time.sleep(0.12)
                return super().group(identity)
        self.spec['durationMs'] = 450
        result, rows = self.run_collection(Slow(self.proc, fixture=True))
        self.assertGreaterEqual(result['skippedSlots'], 1)
        accepted = [r for r in rows if r['kind'] == 'http-resource-sample']
        for first, last in zip(accepted, accepted[1:]):
            self.assertGreater(int(last['startedMonotonicNs']) - int(first['startedMonotonicNs']), 150000000)

    def test_deadline_during_metric_read_is_partial_not_a_missing_metric(self):
        class Slow(C.Kernel):
            def group(inner, identity):
                time.sleep(0.12)
                return super().group(identity)
        self.spec['durationMs'] = 100
        started = time.monotonic()
        result, _ = self.run_collection(Slow(self.proc, fixture=True))
        self.assertEqual(result['reason'], 'deadline-during-sample')
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['samples'], 0)
        self.assertFalse(result['identityContinuous'])
        self.assertLess(time.monotonic() - started, 0.6)

    def test_existing_output_is_untouched_and_inputs_are_strict(self):
        self.out.mkdir()
        (self.out / 'marker').write_text('keep')
        with self.assertRaises(FileExistsError):
            C.collect(self.spec, self.out, kernel=C.Kernel(self.proc, fixture=True))
        self.assertEqual([p.name for p in self.out.iterdir()], ['marker'])
        for key, value in [('intervalMs', 0), ('durationMs', True), ('maxOutputBytes', 100), ('roles', [])]:
            changed = copy.deepcopy(self.spec)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(C.CollectionError):
                C.validate_spec(changed)
        changed = copy.deepcopy(self.spec)
        changed['roles'] = [{'role': 'target', 'cgroupPath': str(self.group)}]
        with self.assertRaises(C.CollectionError):
            C.validate_spec(changed)
        file = self.root / 'duplicate.json'
        file.write_text('{"kind":1,"kind":2}')
        with self.assertRaisesRegex(C.CollectionError, 'duplicate-json-key'):
            C.load_spec(file)

    def controlled(self, root=None):
        root = root or self.root
        stop = root / 'stop.json'
        self.spec['controlledStop'] = {'path': str(stop), 'token': 'a' * 32}
        self.spec['durationMs'] = 800
        return stop

    def publish_stop(self, mutate=None):
        now = str(time.monotonic_ns())
        value = {'schemaVersion': 1, 'kind': 'http-resource-stop', 'token': self.spec['controlledStop']['token'],
                 'specSha256': hashlib.sha256((self.out / 'spec.json').read_bytes()).hexdigest(),
                 'hostBootId': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
                 'workflowFinishedMonotonicNs': now, 'stopRequestedMonotonicNs': now}
        if mutate:
            mutate(value)
        marker = Path(self.spec['controlledStop']['path'])
        temporary = marker.with_suffix('.tmp')
        temporary.write_bytes(C.json_bytes(value))
        os.link(temporary, marker)
        temporary.unlink()
        return value

    def test_controlled_stop_waits_for_complete_post_request_sample_and_seals_receipt(self):
        marker = self.controlled()
        outer = self

        class Publish(C.Kernel):
            calls = 0

            def host(inner, filesystem):
                inner.calls += 1
                if inner.calls == 1:
                    outer.publish_stop()
                return super().host(filesystem)
        result, rows = self.run_collection(Publish(self.proc, fixture=True))
        self.assertEqual((result['status'], result['reason']), ('completed', 'controlled-stop'))
        self.assertLess(int(result['finishedMonotonicNs']) - int(result['startedMonotonicNs']), 600000000)
        receipt = result['controlledStop']
        covered = rows[receipt['coveringSampleIndex']]
        self.assertGreaterEqual(int(covered['startedMonotonicNs']),
                                int(receipt['request']['stopRequestedMonotonicNs']))
        self.assertEqual(receipt['sha256'], hashlib.sha256(marker.read_bytes()).hexdigest())
        self.assertEqual(receipt['request']['specSha256'], result['specSha256'])
        self.assertGreaterEqual(result['samples'], 2)

    def test_controlled_stop_requires_fresh_bound_well_ordered_marker(self):
        for case in ('token', 'specSha256', 'hostBootId', 'future', 'old', 'numeric', 'unknown'):
            with self.subTest(case=case):
                root = self.root / case
                root.mkdir()
                self.proc, self.group, self.spec = fixture(root)
                self.out = root / 'evidence'
                self.controlled(root)
                outer = self

                def mutate(value):
                    if case in ('token', 'specSha256', 'hostBootId'):
                        value[case] = 'mismatched'
                    elif case == 'future':
                        value['stopRequestedMonotonicNs'] = str(time.monotonic_ns() + 10000000000)
                    elif case == 'old':
                        value['workflowFinishedMonotonicNs'] = '0'
                    elif case == 'numeric':
                        value['stopRequestedMonotonicNs'] = time.monotonic_ns()
                    else:
                        value['unexpected'] = True

                class Publish(C.Kernel):
                    def host(inner, filesystem):
                        outer.publish_stop(mutate)
                        return super().host(filesystem)
                result, _ = self.run_collection(Publish(self.proc, fixture=True))
                self.assertEqual(result['status'], 'partial')
                self.assertNotEqual(result['reason'], 'controlled-stop')
                self.assertEqual(result['samples'], 1)

    def test_controlled_stop_final_identity_check_rejects_restart_after_covering_sample(self):
        self.controlled()
        outer = self

        class Restart(C.Kernel):
            reads = 0

            def host(inner, filesystem):
                if not Path(outer.spec['controlledStop']['path']).exists():
                    outer.publish_stop()
                return super().host(filesystem)

            def identity(inner, entry):
                inner.reads += 1
                if inner.reads == 6:
                    path = outer.proc / '123/stat'
                    path.write_text(path.read_text().replace('4321', '4322'))
                return super().identity(entry)
        result, _ = self.run_collection(Restart(self.proc, fixture=True))
        self.assertEqual((result['status'], result['reason']), ('partial', 'role-identity-changed'))
        self.assertEqual(result['samples'], 2)
        self.assertFalse(result['identityContinuous'])
        self.assertNotIn('coveringSampleIndex', result['controlledStop'])

    def test_controlled_stop_missing_preexisting_changed_and_uncovered_are_failures(self):
        marker = self.controlled()
        self.spec['durationMs'] = 100
        result, _ = self.run_collection()
        self.assertEqual((result['status'], result['reason']), ('partial', 'controlled-stop-deadline'))
        self.out = self.root / 'preexisting'
        marker.write_text('{}')
        with self.assertRaisesRegex(C.CollectionError, 'stop-file-already-exists'):
            C.collect(self.spec, self.out, kernel=C.Kernel(self.proc, fixture=True))
        self.assertFalse(self.out.exists())
        marker.unlink()
        self.out = self.root / 'inside'
        self.spec['controlledStop']['path'] = str(self.out)
        with self.assertRaisesRegex(C.CollectionError, 'stop-path-inside-output'):
            C.collect(self.spec, self.out, kernel=C.Kernel(self.proc, fixture=True))
        self.spec['controlledStop']['path'] = str(marker)
        self.out = self.root / 'changed'
        self.spec['durationMs'] = 800
        outer = self

        class Changed(C.Kernel):
            calls = 0

            def host(inner, filesystem):
                inner.calls += 1
                if inner.calls == 1:
                    outer.publish_stop()
                elif inner.calls == 2:
                    marker.unlink()
                return super().host(filesystem)
        result, _ = self.run_collection(Changed(self.proc, fixture=True))
        self.assertEqual((result['status'], result['reason']), ('partial', 'stop-file-changed'))
        self.assertNotIn('coveringSampleIndex', result['controlledStop'])
        self.out = self.root / 'uncovered'
        self.spec['durationMs'] = 100

        class Uncovered(C.Kernel):
            def host(inner, filesystem):
                outer.publish_stop()
                return super().host(filesystem)
        result, _ = self.run_collection(Uncovered(self.proc, fixture=True))
        self.assertEqual((result['status'], result['reason']), ('partial', 'controlled-stop-deadline'))
        self.assertNotIn('coveringSampleIndex', result['controlledStop'])

    def test_reading_special_files_is_rejected_without_blocking(self):
        fifo = self.root / 'pipe'
        os.mkfifo(fifo)
        start = time.monotonic()
        with self.assertRaisesRegex(C.CollectionError, 'not-regular-file'):
            C.read_file(fifo)
        self.assertLess(time.monotonic() - start, 0.2)
        link = self.root / 'link'
        link.symlink_to(self.group / 'cpu.stat')
        with self.assertRaises(C.CollectionError):
            C.read_file(link)

    def test_cli_describe_help_and_signal_cancel_seal_partial(self):
        described = subprocess.run([sys.executable, str(MODULE), 'describe', '--json'],
                                   capture_output=True, text=True, timeout=5)
        self.assertEqual(described.returncode, 0)
        self.assertFalse(json.loads(described.stdout)['performanceAccepted'])
        help_result = subprocess.run([sys.executable, str(MODULE), 'collect', '--help'],
                                    capture_output=True, text=True, timeout=5)
        self.assertEqual(help_result.returncode, 0)
        self.assertIn('never resumes', help_result.stdout)
        self.spec['durationMs'] = 10000
        self.spec['maxSamples'] = 101
        self.controlled()
        self.spec['durationMs'] = 10000
        spec_file = self.root / 'spec.json'
        spec_file.write_text(json.dumps(self.spec))
        launch = self.root / 'launch.py'
        launch.write_text('import importlib.util\n'
                          f's=importlib.util.spec_from_file_location("c",{str(MODULE)!r})\n'
                          'c=importlib.util.module_from_spec(s);s.loader.exec_module(c)\n'
                          'real=c.Kernel\n'
                          f'c.Kernel=lambda:real({str(self.proc)!r},fixture=True)\n'
                          f'raise SystemExit(c.main(["collect","--spec",{str(spec_file)!r},'
                          f'"--out",{str(self.out)!r}]))\n')
        child = subprocess.Popen([sys.executable, str(launch)], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            sample = self.out / 'samples.ndjson'
            while not (sample.exists() and sample.stat().st_size) and time.monotonic() < deadline:
                if child.poll() is not None:
                    break
                time.sleep(0.02)
            self.assertTrue(sample.exists() and sample.stat().st_size, 'CLI never wrote its first sample')
            self.publish_stop()
            child.send_signal(signal.SIGTERM)
            child.send_signal(signal.SIGTERM)
            stdout, stderr = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 2, stderr)
            result = json.loads(stdout)['result']
            self.assertEqual(result['reason'], 'cancelled-sigterm')
            self.assertEqual(result['status'], 'partial')
            self.assertTrue(result['sealed'])
            self.assertTrue((self.out / 'manifest.json').exists())
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()
