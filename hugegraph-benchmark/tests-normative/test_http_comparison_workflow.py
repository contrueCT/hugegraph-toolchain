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

"""Local lifecycle/negative fixtures only; no Docker/HugeGraph performance validation."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

COMPONENT = Path(__file__).resolve().parents[1] / 'components/http/comparison-workflow.py'
SPEC = importlib.util.spec_from_file_location('comparison_workflow_test', COMPONENT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def runtime(root):
    return {'schemaVersion': 1, 'kind': 'http-comparison-runtime', 'sourceRoot': str(root / 'source'),
            'nodeHome': str(root / 'node'), 'hostPython': str(Path(sys.executable).resolve()),
            'dockerExecutable': '/usr/bin/docker',
            'driver': {'image': 'sha256:' + 'a' * 64, 'python': '/usr/bin/python3', 'cpus': 1,
                       'memoryBytes': 512 * MOD.MiB, 'pids': 128, 'tmpfsBytes': 64 * MOD.MiB,
                       'wallSeconds': 600, 'shutdownSeconds': 1, 'maxLogBytes': MOD.MiB},
            'collector': {'intervalMs': 100, 'durationMs': 300000, 'maxSamples': 3001,
                          'maxOutputBytes': MOD.MiB},
            'supervisor': {'pollMs': 100, 'commandMs': 1000, 'maxCommandBytes': MOD.MiB,
                           'minFreeBytes': 0, 'maxWorkBytes': 64 * MOD.MiB}}


class Basics(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        MOD.CANCELLED = False

    def test_runtime_strict_and_resource_constraints(self):
        value = runtime(self.root)
        MOD.validate_runtime(value)
        for mutate in (lambda v: v.update(extra=True), lambda v: v['driver'].update(image='latest'),
                       lambda v: v['driver'].update(cpus=float('nan')),
                       lambda v: v['collector'].update(maxSamples=1),
                       lambda v: v['supervisor'].update(pollMs=1),
                       lambda v: v.update(sourceRoot='relative')):
            changed = json.loads(json.dumps(value))
            mutate(changed)
            with self.assertRaises(MOD.WorkflowError):
                MOD.validate_runtime(changed)

    def test_bounded_regular_inputs_and_duplicate_json(self):
        fifo = self.root / 'fifo'
        os.mkfifo(fifo)
        begun = time.monotonic()
        with self.assertRaises(MOD.WorkflowError):
            MOD.read_bytes(fifo)
        self.assertLess(time.monotonic() - begun, 1)
        path = self.root / 'bad.json'
        path.write_text('{"x":1,"x":2}')
        with self.assertRaises(MOD.WorkflowError):
            MOD.read_json(path)
        path.unlink()
        path.symlink_to(fifo)
        with self.assertRaises(OSError):
            MOD.read_bytes(path)

    def test_release_dependency_lock_uses_fixed_asset_and_rejects_missing_or_changed_bytes(self):
        source = COMPONENT.parents[2]
        package = MOD.read_json(source / 'package.json')
        asset = self.root / 'release/dependency-lock.json'
        asset.parent.mkdir()
        MOD.save(self.root / 'package.json', package)
        raw = MOD.read_bytes(source / 'release/dependency-lock.json')
        asset.write_bytes(raw)
        self.assertEqual(MOD.verify_dependency_lock(self.root), package['hgbenchRelease']['dependencyLock'])
        self.assertIn('release/dependency-lock.json', MOD.SOURCE_FILES)
        self.assertNotIn('package-lock.json', MOD.SOURCE_FILES)
        # An unrelated consumer/source lock cannot replace the packaged fixed asset.
        (self.root / 'package-lock.json').write_bytes(raw)
        asset.unlink()
        with self.assertRaises(FileNotFoundError):
            MOD.verify_dependency_lock(self.root)
        asset.write_bytes(raw + b'\n')
        with self.assertRaisesRegex(MOD.WorkflowError, 'release-dependency-lock-digest'):
            MOD.verify_dependency_lock(self.root)
        asset.write_bytes(raw)
        package['dependencies']['commander'] = '0.0.0'
        MOD.save(self.root / 'package.json', package, replace=True)
        with self.assertRaisesRegex(MOD.WorkflowError, 'release-dependency-lock-declarations'):
            MOD.verify_dependency_lock(self.root)

    def test_optional_jvm_reserves_original_and_parsed_evidence_before_dispatch(self):
        self.assertEqual(MOD.jvm_evidence_reserve({}, {}), 0)
        settings = {'budget': {'maxLogBytes': MOD.MiB}, 'jvmObservation': {
            'maxRawBytes': MOD.MiB, 'parser': {'maxOutputBytes': 4 * MOD.MiB, 'timeoutMs': 5000}}}
        budgets = {'sealAndAnalyzeMs': 90000}
        self.assertEqual(MOD.jvm_evidence_reserve(settings, budgets), 7 * MOD.MiB)
        for changed, error in [
                ({'sealAndAnalyzeMs': 6000}, 'jvm-parser-seal-budget'),
                ({'sealAndAnalyzeMs': 5000}, 'jvm-parser-seal-budget')]:
            with self.assertRaisesRegex(MOD.WorkflowError, error):
                MOD.jvm_evidence_reserve(settings, changed)
        settings['jvmObservation']['maxRawBytes'] += 1
        with self.assertRaisesRegex(MOD.WorkflowError, 'jvm-shared-log-cap'):
            MOD.jvm_evidence_reserve(settings, budgets)
        settings['jvmObservation']['maxRawBytes'] = MOD.MiB
        settings['jvmObservation']['parser']['maxOutputBytes'] = 2 * MOD.MiB
        with self.assertRaisesRegex(MOD.WorkflowError, 'jvm-output-budget'):
            MOD.jvm_evidence_reserve(settings, budgets)
        settings['jvmObservation']['parser']['maxOutputBytes'] = 4 * MOD.MiB
        with self.assertRaisesRegex(MOD.WorkflowError, 'jvm-reverification-seal-budget'):
            MOD.jvm_evidence_reserve(settings, {'sealAndAnalyzeMs': 90000, 'armCount': 4}, 60000)

    def test_atomic_stop_marker_cannot_replace(self):
        path = self.root / 'stop.json'
        MOD.publish_marker(path, {'first': True})
        with self.assertRaises(FileExistsError):
            MOD.publish_marker(path, {'first': False})
        self.assertEqual(MOD.read_json(path), {'first': True})
        self.assertEqual(list(self.root.iterdir()), [path])

    def test_inventory_pins_executable_mode_content_and_internal_links(self):
        source = self.root / 'source'
        source.mkdir()
        path = source / 'program'
        path.write_text('hello')
        (source / 'alias').symlink_to('program')
        first = MOD.inventory(source)
        self.assertEqual(first[0]['target'], 'program')
        path.chmod(0o700)
        self.assertNotEqual(MOD.inventory(source), first)
        (source / 'escape').symlink_to('../outside')
        (self.root / 'outside').touch()
        with self.assertRaises(MOD.WorkflowError):
            MOD.inventory(source)

    def test_selected_inventory_rejects_links_into_unlocked_root_siblings(self):
        source = self.root / 'source'
        (source / 'node_modules').mkdir(parents=True)
        (source / 'vendor/pkg').mkdir(parents=True)
        (source / 'vendor/pkg/code.js').write_text('unlocked')
        (source / 'node_modules/pkg').symlink_to('../vendor/pkg')
        with self.assertRaisesRegex(MOD.WorkflowError, 'target-not-locked'):
            MOD.inventory(source, ('node_modules',))
        self.assertTrue(MOD.inventory(source, ('node_modules', 'vendor')))

    def test_asset_hash_and_copy_check_deadlines_between_chunks(self):
        source = self.root / 'source'
        source.mkdir()
        (source / 'large').write_bytes(b'x' * (2 * MOD.MiB))
        calls = [0]
        def check():
            calls[0] += 1
            if calls[0] >= 3:
                raise MOD.WorkflowError('fixture-deadline')
        with self.assertRaisesRegex(MOD.WorkflowError, 'fixture-deadline'):
            MOD.file_entry(source / 'large', check=check)
        calls[0] = 0
        with self.assertRaisesRegex(MOD.WorkflowError, 'fixture-deadline'):
            MOD.copy_regular_tree(source, self.root / 'copy', 3 * MOD.MiB, check)
        self.assertLess((self.root / 'copy/large').stat().st_size, 2 * MOD.MiB)

    def test_archive_refuses_symlink_and_preserves_bytes(self):
        source = self.root / 'source'
        source.mkdir()
        (source / 'a').write_bytes(b'\x00bytes')
        destination = self.root / 'snapshot'
        MOD.copy_regular_tree(source, destination, 1024)
        self.assertEqual((destination / 'a').read_bytes(), b'\x00bytes')
        with self.assertRaises(MOD.WorkflowError):
            MOD.copy_regular_tree(source, destination, 1024)
        (source / 'link').symlink_to('a')
        with self.assertRaises(MOD.WorkflowError):
            MOD.copy_regular_tree(source, self.root / 'bad', 1024)

    def test_value_hash_matches_node_ascii_lock(self):
        node = __import__('shutil').which('node')
        if not node:
            self.skipTest('Node not installed')
        files = [{'path': 'a/b.json', 'sha256': 'a' * 64, 'bytes': 100}]
        code = ('import {valueHash} from "./components/paired/core.mjs";'
                'console.log(valueHash(JSON.parse(process.argv[1])))')
        result = subprocess.check_output([node, '--input-type=module', '-e', code, json.dumps(files)],
                                         cwd=COMPONENT.parents[2], text=True)
        self.assertEqual(MOD.value_hash(files), result.strip())

    def test_command_preserves_stdout_error_and_output_cap(self):
        value = MOD.command([sys.executable, '-c', 'import sys;print("structured-error");sys.exit(6)'],
                            2, 4096, dict(os.environ), check=False)
        self.assertEqual(value['exitCode'], 6)
        self.assertIn('structured-error', value['stdout'])
        record = self.root / 'oversize.json'
        with self.assertRaisesRegex(MOD.WorkflowError, 'command-output-budget'):
            MOD.command([sys.executable, '-c', 'print("x"*10000)'], 2, 128, dict(os.environ), record=record)
        self.assertLessEqual(len(MOD.read_json(record)['stdout']), 128)

    def test_command_guard_prevents_spawn(self):
        marker = self.root / 'spawned'
        def cancel():
            raise MOD.WorkflowError('cancelled')
        with self.assertRaisesRegex(MOD.WorkflowError, 'cancelled'):
            MOD.command([sys.executable, '-c', 'open(' + repr(str(marker)) + ',"w").close()'],
                        1, 4096, dict(os.environ), guard=cancel)
        self.assertFalse(marker.exists())

    def test_timeout_kills_descendant_after_parent_exit(self):
        pidfile = self.root / 'child.pid'
        code = ('import os,time; p=os.fork(); '
                '\nif p==0:\n open(' + repr(str(pidfile)) + ',"w").write(str(os.getpid()))\n time.sleep(30)\n'
                'else:\n os._exit(0)\n')
        record = self.root / 'command.json'
        with self.assertRaisesRegex(MOD.WorkflowError, 'command-deadline-exceeded'):
            MOD.command([sys.executable, '-c', code], 0.4, 4096, dict(os.environ), record=record)
        pid = int(pidfile.read_text())
        self.addCleanup(lambda: self.kill_if_alive(pid))
        end = time.monotonic() + 2
        while time.monotonic() < end and self.alive(pid):
            time.sleep(0.02)
        self.assertFalse(self.alive(pid))
        self.assertEqual(MOD.read_json(record)['exitCode'], 0)

    @staticmethod
    def alive(pid):
        if sys.platform.startswith('linux'):
            try:
                return Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[0] not in ('Z', 'X')
            except FileNotFoundError:
                return False
        result = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True)
        return bool(result.stdout.strip()) and not result.stdout.strip().startswith('Z')

    @staticmethod
    def kill_if_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def test_sigterm_cancels_command_and_seals_partial_log(self):
        record = self.root / 'signal.json'
        ready = self.root / 'sleep-ready.pid'
        child = ('import os,time;from pathlib import Path;'
                 'Path(' + repr(str(ready) + '.tmp') + ').write_text(str(os.getpid()));'
                 'os.replace(' + repr(str(ready) + '.tmp') + ',' + repr(str(ready)) + ');time.sleep(30)')
        argv = [sys.executable, '-B', '-c', child]
        code = ('import importlib.util,signal;'
                's=importlib.util.spec_from_file_location("w",' + repr(str(COMPONENT)) + ');'
                'm=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
                'signal.signal(signal.SIGTERM,m.signal_cancel);'
                '\ndef guard():\n m.need(not m.CANCELLED,"cancelled")\n'
                'm.command(' + repr(argv) + ',30,4096,{},guard=guard,record=' + repr(str(record)) + ')')
        process = subprocess.Popen([sys.executable, '-B', '-c', code], stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        # Readiness comes from the running command, after the supervisor installed
        # its handler. A fixed delay can signal Python before import/handler setup.
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline and process.poll() is None:
            time.sleep(0.01)
        self.assertTrue(ready.exists(), 'supervised command did not reach readiness')
        child_pid = int(ready.read_text())
        self.addCleanup(lambda: self.kill_if_alive(child_pid))
        process.terminate()
        process.wait(timeout=3)
        self.assertEqual(MOD.read_json(record)['error'], 'cancelled')
        self.assertFalse(self.alive(child_pid))

    def test_collector_start_failure_uses_term_and_allows_partial_evidence(self):
        controller = FakeController(self.root)
        controller.env = dict(os.environ)
        work = self.root / 'collector-work'
        sealed = self.root / 'collector-sealed'
        work.mkdir()
        (sealed / 'controller').mkdir(parents=True)
        marker = self.root / 'partial-marker'
        ready = self.root / 'signal-ready'
        code = ('import signal,time,pathlib;'
                'signal.signal(signal.SIGTERM,lambda *a:(pathlib.Path(' + repr(str(marker)) + ').write_text("partial"),'
                'exit(0)));pathlib.Path(' + repr(str(ready)) + ').touch();time.sleep(30)')
        process = subprocess.Popen([sys.executable, '-c', code], start_new_session=True)
        self.addCleanup(lambda: MOD.kill_group(process) if process.poll() is None else None)
        deadline = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(ready.exists())
        with patch.object(MOD.subprocess, 'Popen', return_value=process), \
             patch.object(MOD, 'first_sample', side_effect=MOD.WorkflowError('cancelled-before-first-sample')):
            with self.assertRaisesRegex(MOD.WorkflowError, 'cancelled-before-first-sample'):
                MOD.start_collector(controller, {'runId': 'fixture-arm'}, work, sealed, 123, 456)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(marker.read_text(), 'partial')

    def test_help_describe_and_error_are_machine_readable(self):
        result = subprocess.run([sys.executable, '-B', COMPONENT, '--help'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn('single-dispatch', result.stdout)
        result = subprocess.run([sys.executable, '-B', COMPONENT, 'describe'], capture_output=True, text=True)
        self.assertFalse(json.loads(result.stdout)['data']['performanceAccepted'])
        result = subprocess.run([sys.executable, '-B', COMPONENT, 'status', '--attempt', self.root],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 6)
        self.assertFalse(json.loads(result.stdout)['error']['retryable'])


class ActiveDirectorySampling(unittest.TestCase):
    setUp = Basics.setUp

    def test_enumerated_vanished_files_are_counted_with_bounded_paths(self):
        for index in range(20):
            (self.root / ('temporary-' + str(index))).write_bytes(b'x')
        scanner = MOD.ActiveUsage(self.root, 100, 'fixture-byte-budget', 0)
        original_stat = os.stat

        def disappear(path, *args, **kwargs):
            if kwargs.get('dir_fd') is not None:
                os.unlink(path, dir_fd=kwargs['dir_fd'])
            return original_stat(path, *args, **kwargs)

        with patch.object(MOD.os, 'stat', side_effect=disappear):
            sample = scanner.sample()
        self.assertEqual(sample['outcome'], 'within-budget')
        self.assertEqual(sample['vanishedEntries'], 20)
        self.assertEqual(sample['observedBytes'], 0)
        self.assertEqual(len(sample['vanishedPathSamples']), scanner.MAX_PATH_SAMPLES)
        self.assertEqual(scanner.audit['vanishedEntries'], 20)
        self.assertTrue(all(p['path'].startswith('temporary-') for p in sample['vanishedPathSamples']))
        self.assertLess(len(MOD.encode(scanner.audit)), 16384)

    def test_actual_atomic_create_rename_delete_pressure_does_not_require_quiescence(self):
        (self.root / 'stable').write_bytes(b'x' * 4096)
        done, started = threading.Event(), threading.Event()
        operations, errors = [0], []

        def writer():
            try:
                while not done.is_set():
                    for index in range(16):
                        temporary = self.root / ('active-' + str(index) + '.tmp')
                        temporary.write_bytes(b'x' * 128)
                        target = temporary.with_suffix('.json')
                        os.replace(temporary, target)
                        target.unlink()
                        operations[0] += 1
                        started.set()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        try:
            self.assertTrue(started.wait(2))
            scanner = MOD.ActiveUsage(self.root, MOD.MiB, 'fixture-byte-budget', 0)
            for _ in range(100):
                sample = scanner.sample()
                self.assertEqual(sample['outcome'], 'within-budget')
                self.assertGreaterEqual(sample['observedBytes'], 4096)
            self.assertGreater(operations[0], 0)
            self.assertEqual(scanner.audit['scans'], 100)
        finally:
            done.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_permission_and_other_io_errors_are_not_vanished(self):
        (self.root / 'protected').write_text('keep')
        for error in (PermissionError(13, 'fixture permission denied'), OSError(5, 'fixture I/O error')):
            scanner = MOD.ActiveUsage(self.root, 100, 'fixture-byte-budget')
            with patch.object(MOD.os, 'stat', side_effect=error):
                with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-io-error'):
                    scanner.sample()
            self.assertEqual(scanner.audit['latest']['errno'], error.errno)
            self.assertEqual(scanner.audit['latest']['failurePath']['path'], 'protected')
            self.assertEqual(scanner.audit['vanishedEntries'], 0)

    def test_real_permission_denied_directory_is_rejected(self):
        if os.geteuid() == 0:
            self.skipTest('permission denial requires nonroot; injected EACCES is covered separately')
        denied = self.root / 'denied'
        denied.mkdir()
        denied.chmod(0)
        try:
            scanner = MOD.ActiveUsage(self.root, 100, 'fixture-byte-budget')
            with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-io-error'):
                scanner.sample()
            self.assertEqual(scanner.audit['latest']['errno'], 13)
        finally:
            denied.chmod(0o700)

    def test_root_replacement_during_or_between_scans_is_rejected(self):
        active = self.root / 'active'
        active.mkdir()
        (active / 'file').write_text('keep')
        scanner = MOD.ActiveUsage(active, 100, 'fixture-byte-budget')
        scanner.sample()
        original_stat = os.stat
        moved = [False]

        def replace_root(path, *args, **kwargs):
            if kwargs.get('dir_fd') is not None and not moved[0]:
                active.rename(self.root / 'old')
                active.mkdir()
                moved[0] = True
            return original_stat(path, *args, **kwargs)

        with patch.object(MOD.os, 'stat', side_effect=replace_root):
            with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-root-identity-changed'):
                scanner.sample()
        with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-root-identity-changed'):
            scanner.sample()
        active.rmdir()
        with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-io-error'):
            scanner.sample()
        self.assertEqual(scanner.audit['vanishedEntries'], 0)

    def test_links_special_files_and_replaced_child_directories_fail_closed(self):
        link = self.root / 'link'
        link.symlink_to('missing')
        for create in (lambda: None, lambda: os.mkfifo(link)):
            create()
            with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-link-or-special-file'):
                MOD.ActiveUsage(self.root, 100, 'fixture-budget').sample()
            link.unlink()
        directory = self.root / 'directory'
        directory.mkdir()
        original_open = os.open

        def replace_child(path, *args, **kwargs):
            if path == 'directory' and kwargs.get('dir_fd') is not None:
                directory.rename(self.root / 'old-directory')
                directory.mkdir()
            return original_open(path, *args, **kwargs)

        with patch.object(MOD.os, 'open', side_effect=replace_child):
            with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-directory-identity-changed'):
                MOD.ActiveUsage(self.root, 100, 'fixture-budget').sample()

    def test_byte_entry_depth_time_and_disk_budgets_remain_independent(self):
        (self.root / 'file').write_bytes(b'x' * 10)
        scanner = MOD.ActiveUsage(self.root, 9, 'fixture-byte-budget')
        with self.assertRaisesRegex(MOD.WorkflowError, 'fixture-byte-budget'):
            scanner.sample()
        self.assertEqual(scanner.audit['latest']['observedBytes'], 10)
        self.assertEqual(scanner.audit['latest']['outcome'], 'failed')
        scanner = MOD.ActiveUsage(self.root, 100, 'fixture-byte-budget')
        scanner.MAX_ENTRIES = 0
        with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-entry-limit'):
            scanner.sample()
        scanner.MAX_ENTRIES = 100
        scanner.MAX_SECONDS = 0
        with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-deadline'):
            scanner.sample()
        (self.root / 'nested').mkdir()
        scanner.MAX_SECONDS = 5
        scanner.MAX_DEPTH = 0
        with self.assertRaisesRegex(MOD.WorkflowError, 'active-scan-depth-limit'):
            scanner.sample()
        scanner = MOD.ActiveUsage(self.root, 100, 'fixture-byte-budget', 1 << 62)
        with self.assertRaisesRegex(MOD.WorkflowError, 'disk-reserve'):
            scanner.sample()
        self.assertEqual(scanner.audit['latest']['entries'], 0)
        self.assertIsNotNone(scanner.audit['latest']['freeBytes'])

    def test_scan_checks_external_deadline_and_immutable_scan_stays_strict(self):
        (self.root / 'file').write_text('keep')
        def cancelled():
            raise MOD.WorkflowError('fixture-cancelled')
        scanner = MOD.ActiveUsage(self.root, 100, 'fixture-byte-budget')
        with self.assertRaisesRegex(MOD.WorkflowError, 'fixture-cancelled'):
            scanner.sample(cancelled)
        with patch.object(MOD.Path, 'lstat', side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                MOD.regular_files(self.root)


class FakeProcess:
    returncode = 0
    def poll(self):
        return None


class FakeController(MOD.Controller):
    """Stateful local fixture of public command boundaries; does not prove Docker execution."""
    def __init__(self, root, fail=None):
        self.root = root
        self.settings = runtime(root)
        self.runtime = {'settings': self.settings, 'identities': {'javaHome': [], 'distribution': []}}
        self.source = root / 'source'
        self.node = root / 'node/bin/node'
        self.plan = {'planDigest': 'a' * 64, 'budgets': {'modeProbeMs': 30000, 'prepareMs': 300000,
                     'startMs': 300000, 'settleMs': 0, 'stopMs': 30000, 'sealAndAnalyzeMs': 30000}}
        self.shared = {'image': 'sha256:' + 'a' * 64, 'python': '/usr/bin/python3',
                       'dockerSocket': '/var/run/docker.sock'}
        self.execution = {'executionId': 'a' * 32, 'hostBootId': 'b' * 36}
        self.digest = 'c' * 64
        self.arm_deadline = None
        self.cleanup = False
        self.events = []
        self.fail = fail
        self.driver_started = False
        self.work = None
        self.pending = 2
        for name in ('work', 'execution/arms', 'comparison/inputs'):
            (root / name).mkdir(parents=True, exist_ok=True)
        MOD.save(root / 'runtime.json', self.runtime)
        MOD.save(root / 'runtime.lock.json', {'fixture': True})
        MOD.save(root / 'comparison/inputs/build.json', {'files': []})
        self.active_usage = MOD.ActiveUsage(root, 64 * MOD.MiB, 'work-directory-budget', 0)

    def guard(self):
        if not self.cleanup:
            MOD.need(not MOD.CANCELLED, 'controller-cancelled')

    def pause(self, seconds):
        self.guard()

    def cli(self, args, timeout=None, log=None):
        words = list(map(str, args))
        if words[:3] == ['http', 'environment', 'plan']:
            self.events.append('prepare')
            attempt = Path(words[-1])
            self.work = attempt.parent
            attempt.mkdir()
            (attempt / 'work').mkdir()
            (attempt / 'work/rocksdb').mkdir()
            (attempt / 'work/rocksdb/DATA').write_text('never archive this graph data')
            settings = MOD.read_json(words[3])
            MOD.save(attempt / 'plan.json', {'settings': settings, 'sources': {'javaHome': {'files': []},
                      'distribution': {'files': []}}, 'controlFiles': []})
            return {'taskPlan': str(attempt / 'task.json')}
        if words[:3] == ['http', 'environment', 'status']:
            self.events.append('status')
            if self.pending:
                self.pending -= 1
                return {'state': 'dispatch-recorded', 'container': None}
            return {'state': 'running', 'ready': {'java': {'pid': 123}},
                    'container': {'id': 'd' * 64}}
        if 'run' in words:
            self.events.append('target-dispatch')
            return {'taskId': 'fixture'}
        raise AssertionError(words)

    def docker(self, *args, check=True, log=None, timeout=None):
        if args[0] == 'create':
            self.events.append('driver-create')
            if self.fail == 'driver-create':
                raise MOD.WorkflowError('ambiguous-create-result')
        if args[0] == 'start':
            self.events.append('driver-start')
            self.driver_started = True
            MOD.save(self.work / 'driver/driver-ready.json', {'fixture': True, 'pid': 234, 'startTimeTicks': '567',
                      'runId': self.work.name, 'bootId': self.execution['hostBootId']})
        if args[0] == 'inspect':
            return {'stdout': json.dumps([{'State': {'Pid': 12}}]), 'exitCode': 0}
        return {'stdout': '', 'exitCode': 0}

    def inspect_driver(self, arm, missing=False):
        evidence = self.work / 'driver'
        if (evidence / 'go.json').exists() and not (evidence / 'workflow-result.json').exists():
            self.events.append('workflow')
            out = evidence / 'attempt/output'
            out.mkdir(parents=True)
            MOD.save(out / 'raw.lock.json', {'fixture': True})
            MOD.save(evidence / 'workflow-result.json', {'ok': True, 'runId': arm['runId'],
                      'startedMonotonicNs': MOD.ns(), 'finishedMonotonicNs': MOD.ns()})
        return {'Id': 'd' * 64, 'State': {'Pid': 123, 'Running': not (evidence / 'release.json').exists(),
                                        'ExitCode': 0, 'OOMKilled': False}}

    def verify_driver(self, info, arm, target, evidence):
        self.events.append('driver-verify')

    def mode_probe(self, attempt, observed, arm, phase, logs):
        self.events.append('mode-' + phase)
        if self.fail == phase:
            raise MOD.WorkflowError('mode-unverified')
        output = attempt / 'work' / ('mode-' + phase)
        output.mkdir()
        MOD.save(output / 'result.json', {'status': 'passed', 'fixture': True})

    def find_host_java(self, init_pid, ready):
        return 456

    def start_collector(self, *args):
        self.events.append('collector-first-sample')
        return FakeProcess()

    def finish_collector(self, *args):
        self.events.append('collector-controlled-stop')

    def stop_driver(self, arm, logs):
        self.events.append('driver-stop')
        if self.fail == 'driver-stop':
            raise MOD.WorkflowError('driver-stop-failed')

    def stop_target(self, work, logs):
        self.events.append('target-stop')
        if self.fail == 'target-stop':
            raise MOD.WorkflowError('target-stop-failed')


class Lifecycle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.arm = {'runId': 'arm-' + '1' * 40, 'blockId': 'b01', 'arm': 'A', 'position': 0,
                    'expectedMode': 'forward', 'maxMilliseconds': 300000, 'maxEvidenceBytes': 32 * MOD.MiB}
        MOD.CANCELLED = False
        identity = patch.object(MOD, 'process_identity', side_effect=lambda role, pid: {'role': role, 'hostPid': pid})
        identity.start()
        self.addCleanup(identity.stop)

    def test_order_pending_create_window_receipt_and_small_archive(self):
        controller = FakeController(self.root)
        controller.arm(self.arm)
        events = controller.events
        self.assertEqual(events.count('target-dispatch'), 1)
        self.assertEqual(events.count('driver-create'), 1)
        self.assertEqual(events.count('status'), 3)
        for before, after in [('prepare', 'target-dispatch'), ('mode-pre', 'driver-start'),
                              ('collector-first-sample', 'workflow'), ('workflow', 'collector-controlled-stop'),
                              ('collector-controlled-stop', 'mode-post'), ('mode-post', 'target-stop')]:
            self.assertLess(events.index(before), events.index(after))
        archive = self.root / 'execution/arms' / self.arm['runId']
        receipt = MOD.read_json(archive / 'receipt.json')
        self.assertEqual(receipt['runId'], self.arm['runId'])
        self.assertIsNone(receipt['failure'])
        self.assertEqual(receipt['cleanupErrors'], [])
        self.assertFalse((archive / 'environment/work/rocksdb').exists())
        self.assertTrue((archive / 'environment/work/mode-pre/result.json').exists())
        self.assertTrue(MOD.read_json(archive / 'controller/host-active-usage.json')['samplingOnly'])
        self.assertLessEqual(int(receipt['armStartedMonotonicNs']), int(receipt['workflowStartedMonotonicNs']))
        self.assertLessEqual(int(receipt['workflowFinishedMonotonicNs']), int(receipt['armFinishedMonotonicNs']))
        with self.assertRaises(FileExistsError):
            controller.arm(self.arm)
        self.assertEqual(events.count('target-dispatch'), 1)

    def test_preprobe_failure_never_dispatches_driver_and_preserves_failed_arm(self):
        controller = FakeController(self.root, 'pre')
        with self.assertRaises(MOD.WorkflowError):
            controller.arm(self.arm)
        self.assertNotIn('driver-create', controller.events)
        self.assertIn('target-stop', controller.events)
        receipt = MOD.read_json(self.root / 'execution/arms' / self.arm['runId'] / 'receipt.json')
        self.assertEqual(receipt['failure'], 'mode-unverified')
        self.assertIsNone(receipt['rawLockSha256'])

    def test_jvm_parse_failure_preserves_http_raw_and_lifecycle_evidence(self):
        controller = FakeController(self.root)
        with patch.object(controller, 'parse_jvm', side_effect=MOD.WorkflowError('injected-jvm-parse-failure')):
            with self.assertRaises(MOD.WorkflowError):
                controller.arm(self.arm)
        archive = self.root / 'execution/arms' / self.arm['runId']
        receipt = MOD.read_json(archive / 'receipt.json')
        self.assertEqual(receipt['cleanupErrors'], ['injected-jvm-parse-failure'])
        self.assertIsNotNone(receipt['rawLockSha256'])
        self.assertTrue((archive / 'raw/raw.lock.json').is_file())
        self.assertTrue((archive / 'environment/work/mode-post/result.json').is_file())
        self.assertTrue((archive / 'controller/workflow-result.json').is_file())
        self.assertIn('target-stop', controller.events)
        self.assertIn('driver-stop', controller.events)

    def test_late_ready_is_rejected_even_when_status_returns_ready(self):
        controller = FakeController(self.root)
        def delayed(*args, **kwargs):
            time.sleep(0.02)
            return {'state': 'running', 'ready': {'fixture': True}}
        controller.cli = delayed
        with self.assertRaisesRegex(MOD.WorkflowError, 'deadline'):
            controller.wait_ready(self.root, 1)
        with self.assertRaisesRegex(MOD.WorkflowError, 'budget-exhausted'):
            controller.wait_ready(self.root, -1)

    def test_ambiguous_driver_create_does_not_retry(self):
        controller = FakeController(self.root, 'driver-create')
        with self.assertRaises(MOD.WorkflowError):
            controller.arm(self.arm)
        self.assertEqual(controller.events.count('driver-create'), 1)
        self.assertNotIn('driver-start', controller.events)
        self.assertIn('driver-stop', controller.events)
        self.assertIn('target-stop', controller.events)

    def test_driver_cleanup_failure_never_skips_target_cleanup(self):
        controller = FakeController(self.root, 'driver-stop')
        with self.assertRaises(MOD.WorkflowError):
            controller.arm(self.arm)
        self.assertIn('target-stop', controller.events)
        receipt = MOD.read_json(self.root / 'execution/arms' / self.arm['runId'] / 'receipt.json')
        self.assertEqual(receipt['cleanupErrors'], ['driver-stop-failed'])

    def test_driver_command_locks_identity_readonly_resources_and_no_socket(self):
        controller = FakeController(self.root)
        argv = controller.driver_arguments(self.arm, 'd' * 64, self.root / 'driver')
        self.assertIn('--pull=never', argv)
        self.assertIn('container:' + 'd' * 64, argv)
        self.assertNotIn('/var/run/docker.sock', ' '.join(argv))
        self.assertEqual(argv[argv.index('--memory') + 1], str(512 * MOD.MiB))
        self.assertEqual(argv[argv.index('--memory-swap') + 1], str(512 * MOD.MiB))
        for dest in ('/source', '/node', '/comparison', '/control'):
            self.assertTrue(any('dst=' + dest + ',readonly' in part for part in argv))
        self.assertIn(controller.digest, ' '.join(argv))

    def test_generated_driver_argv_matches_strict_docker_inspect_contract(self):
        controller = FakeController(self.root)
        target, evidence = 'd' * 64, self.root / 'driver'
        argv = controller.driver_arguments(self.arm, target, evidence)
        option = lambda name: argv[argv.index(name) + 1]
        self.assertEqual(option('--security-opt'), 'no-new-privileges:true')
        mounts = []
        for index, item in enumerate(argv):
            if item == '--mount':
                pieces = argv[index + 1].split(',')
                values = dict(piece.split('=', 1) for piece in pieces if '=' in piece)
                mounts.append({'Type': values['type'], 'Source': values['src'],
                               'Destination': values['dst'], 'RW': 'readonly' not in pieces})
        # Docker 29 preserves the security option string, as observed in the failed v1 creation.
        # Exercise the actual argv producer against the real strict consumer, without Docker effects.
        info = {'HostConfig': {
                    'ReadonlyRootfs': '--read-only' in argv, 'NetworkMode': option('--network'),
                    'Memory': int(option('--memory')), 'MemorySwap': int(option('--memory-swap')),
                    'NanoCpus': round(float(option('--cpus')) * 1e9), 'PidsLimit': int(option('--pids-limit')),
                    'CapDrop': [option('--cap-drop')], 'CapAdd': [], 'Privileged': False,
                    'SecurityOpt': [option('--security-opt')], 'RestartPolicy': {'Name': option('--restart')},
                    'LogConfig': {'Type': option('--log-driver')}, 'Init': '--init' in argv,
                    'PidMode': '', 'Devices': [], 'PortBindings': {}, 'PublishAllPorts': False},
                'Config': {'User': option('--user'), 'Entrypoint': [option('--entrypoint')],
                           'Cmd': argv[argv.index(controller.settings['driver']['image']) + 1:],
                           'WorkingDir': option('--workdir')}, 'Mounts': mounts}
        MOD.Controller.verify_driver(controller, info, self.arm, target, evidence)
        info['HostConfig']['SecurityOpt'] = ['no-new-privileges']
        with self.assertRaisesRegex(MOD.WorkflowError, 'driver-isolation-mismatch'):
            MOD.Controller.verify_driver(controller, info, self.arm, target, evidence)

    def test_foreign_driver_identity_is_never_returned_for_cleanup(self):
        controller = FakeController(self.root)
        info = {'Name': '/foreign', 'Image': controller.settings['driver']['image'], 'Config': {'Labels': {}}}
        controller.docker = lambda *a, **k: {'exitCode': 0, 'stdout': json.dumps([info]), 'stderr': ''}
        with self.assertRaisesRegex(MOD.WorkflowError, 'driver-owner-identity-mismatch'):
            MOD.Controller.inspect_driver(controller, self.arm)


class CollectorSeal(unittest.TestCase):
    setUp = Basics.setUp
    def test_collector_partial_or_tampered_manifest_rejected(self):
        root = self.root
        for name, value in [('spec.json', {'fixture': True}), ('summary.json', {'status': 'completed',
                            'reason': 'controlled-stop', 'firstSampleStartedMonotonicNs': '10',
                            'lastSampleFinishedMonotonicNs': '30'})]:
            MOD.save(root / name, value)
        (root / 'samples.ndjson').write_text(json.dumps({'kind': 'http-resource-sample',
            'startedMonotonicNs': '10', 'finishedMonotonicNs': '12'}) + '\n')
        manifest = {'kind': 'http-resource-evidence', 'sealed': True, 'collectionStatus': 'completed',
                    'files': [MOD.file_entry(root / name, name)
                              for name in ('spec.json', 'summary.json', 'samples.ndjson')]}
        MOD.save(root / 'manifest.json', manifest)
        MOD.verify_collector(root, {'startedMonotonicNs': '15', 'finishedMonotonicNs': '25'})
        with self.assertRaisesRegex(MOD.WorkflowError, 'window-uncovered'):
            MOD.verify_collector(root, {'startedMonotonicNs': '5', 'finishedMonotonicNs': '25'})
        (root / 'samples.ndjson').write_text('changed')
        with self.assertRaisesRegex(MOD.WorkflowError, 'manifest-digest'):
            MOD.verify_collector(root)


class RunFixture(FakeController):
    def __init__(self, root, bridge_valid=True, bridge_exit=0):
        super().__init__(root)
        __import__('shutil').rmtree(root / 'work')
        __import__('shutil').rmtree(root / 'execution')
        self.runtime.update(planDigest='a' * 64, planContentDigest='b' * 64,
                            componentSha256=MOD.file_entry(COMPONENT)['sha256'])
        self.started = time.monotonic()
        self.plan.update(analysisPolicy={'maxMilliseconds': 100})
        self.plan['budgets'].update(maxTotalEvidenceBytes=32 * MOD.MiB, maxTotalMilliseconds=300000)
        MOD.save(root / 'comparison/inputs/environment.json', {
            'hostBootId': 'host-boot-fixture', 'kernel': {'release': 'fixture', 'architecture': 'fixture'},
            'node': {'version': 'v22.0.0'}, 'docker': {'serverVersion': '29.0.0'}})
        self.bridge_valid, self.bridge_exit = bridge_valid, bridge_exit
        self.order = []

    def planned_arms(self):
        return [{'runId': 'arm-01', 'arm': 'B'}, {'runId': 'arm-02', 'arm': 'A'}]

    def arm(self, arm):
        registered = MOD.read_json(self.root / 'execution/execution.json')
        self.assert_registration = len(registered['receipts']) == 2
        self.order.append(arm['runId'])

    def cmd(self, argv, *args, **kwargs):
        if '--version' in argv:
            return {'exitCode': 0, 'stdout': 'v22.0.0\n', 'stderr': ''}
        return {'exitCode': self.bridge_exit, 'stdout': json.dumps({'ok': True,
                'data': {'valid': self.bridge_valid}}), 'stderr': ''}

    def docker(self, *args, **kwargs):
        return {'exitCode': 0, 'stdout': '{"Version":"29.0.0"}', 'stderr': ''}

    def cli(self, *args, **kwargs):
        return {'planDigest': 'a' * 64, 'contentDigest': 'b' * 64}


class Registration(unittest.TestCase):
    setUp = Basics.setUp

    def execute(self, valid=True, exit_code=0, reverify_failure=False):
        controller = RunFixture(self.root, valid, exit_code)
        def verify(*args, **kwargs):
            if reverify_failure:
                raise MOD.WorkflowError('runtime-identity-changed')
        with patch.object(MOD.platform, 'system', return_value='Linux'), \
             patch.object(MOD.platform, 'release', return_value='fixture'), \
             patch.object(MOD.platform, 'machine', return_value='fixture'), \
             patch.object(MOD.os, 'getuid', return_value=1000), \
             patch.object(MOD.os, 'getgid', return_value=1000), \
             patch.object(MOD, 'proc_read', return_value='host-boot-fixture'), \
             patch.object(MOD, 'load_attempt', side_effect=verify):
            result = controller.run()
            with self.assertRaises(FileExistsError):
                controller.run()
        self.assertEqual(controller.order, ['arm-01', 'arm-02'])
        self.assertTrue(controller.assert_registration)
        lock = MOD.read_json(self.root / 'execution/execution.lock.json')
        self.assertIn('terminal.json', [f['path'] for f in lock['files']])
        self.assertEqual(lock['contentDigest'], MOD.value_hash(lock['files']))
        return result

    def test_registration_precedes_first_dispatch_and_balanced_plan_order_is_obeyed(self):
        result = self.execute()
        self.assertTrue(result['ok'])
        self.assertTrue(result['executionCompleted'])
        self.assertTrue(result['evidenceValid'])
        self.assertFalse(result['performanceAccepted'])

    def test_bridge_nonzero_makes_overall_result_failed(self):
        result = self.execute(exit_code=6)
        self.assertFalse(result['ok'])
        self.assertTrue(result['executionCompleted'])
        self.assertFalse(result['evidenceValid'])

    def test_bridge_invalid_is_not_success(self):
        result = self.execute(valid=False)
        self.assertFalse(result['ok'])
        self.assertFalse(result['evidenceValid'])

    def test_runtime_end_drift_is_in_locked_terminal(self):
        result = self.execute(reverify_failure=True)
        self.assertFalse(result['ok'])
        terminal = MOD.read_json(self.root / 'execution/terminal.json')
        self.assertEqual(terminal['status'], 'failed')
        self.assertFalse(terminal['runtimeReverified'])
        self.assertEqual(terminal['failure'], 'runtime-identity-changed')


if __name__ == '__main__':
    unittest.main()
