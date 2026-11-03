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

"""Local protocol fixtures. No Docker, SSH, HugeGraph or performance proof."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
FILE = ROOT / '.hgbench/http-driver-headroom-execute.py'
SPEC = importlib.util.spec_from_file_location('headroom_executor_tests', FILE)
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
PLAN = ROOT / '.hgbench/http-driver-headroom-draft-v2'


class HeadroomExecute(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hgbench-headroom-execute-')
        self.root = Path(self.temp.name)
        M.K.SIGNAL = None

    def tearDown(self):
        M.K.SIGNAL = None
        self.temp.cleanup()

    def freeze(self):
        output = self.root / 'execution'
        M.freeze(PLAN, M.sha(M.raw_file(PLAN / 'plan.lock.json')), output)
        return output

    def test_actual_offline_draft_freeze_rejects_execution_and_resealed_budget_override(self):
        output = self.freeze()
        spec = M.read(output / 'execution-spec.json')
        self.assertFalse(spec['runnable'])
        self.assertEqual(spec['fixedLifecycleSha256'], M.KNOWN_SHA)
        self.assertEqual(spec['budgets']['driverLifetimeMs'], 750000)
        self.assertEqual(spec['budgets']['httpWaitMs'], 644000)
        self.assertEqual(spec['physicalArms'], 12)
        self.assertEqual(len({r[k] for r in spec['expectedTasks'] for k in ('targetTaskId', 'httpTaskId')}), 24)
        with self.assertRaisesRegex(RuntimeError, 'prepared-linux-owner-required'):
            M.Operation(output).run()
        self.assertFalse((output / 'dispatch.json').exists())
        changed = copy.deepcopy(spec)
        changed['budgets']['httpWaitMs'] += 100000
        M.save(output / 'execution-spec.json', changed, True)
        M.save(output / 'execution.lock.json', {'kind': 'http-driver-headroom-execution-lock',
               'specSha256': M.sha(M.raw_file(output / 'execution-spec.json'))}, True)
        with self.assertRaisesRegex(RuntimeError, 'execution-runtime-or-fixed-spec-differs'):
            M.load_execution(output)

    def run_fixture(self, pressure=None, failure=None):
        op = object.__new__(M.Operation)
        op.root, op.plan = self.root / 'execution', self.root / 'prepared'
        op.root.mkdir()
        op.plan.mkdir()
        executable = self.root / 'node-fixture'
        executable.write_bytes(b'synthetic-not-a-runtime')
        M.save(op.root / 'execution-spec.json', {'fixture': True})
        op.spec = {'runnable': True, 'owner': '%d:%d' % (os.getuid(), os.getgid()),
                   'node': str(executable), 'nodeExecutable': {'sha256': M.sha(executable.read_bytes())}}
        g = M.generator()
        op.policy, op.b = g.analysis_contract(), M.fixed_budgets(g)
        arms = M.read(PLAN / 'sequence.json')['physicalArms']
        op.sequence = {'physicalArms': copy.deepcopy(arms)}
        op.rows = [{**{k: a[k] for k in ('runId', 'cellId', 'blockId', 'ratePerSecond', 'driverCpus')},
                    'status': 'not-started', 'issues': [], 'cells': []} for a in arms]
        op.current = None
        op.command_count = op.combined_peak = 0
        op.usage, op.arm_usage = [], []
        op.start = time.monotonic()
        op.deadline = op.start + 60
        op.cleanup_deadline = op.step_deadline = None
        op.barred_above = None
        op.guard = lambda: op.basic()
        calls = []

        def run_arm(arm, row):
            calls.append(arm['cellId'])
            op.current = {'arm': arm, 'targetRequested': False, 'driverRequested': False, 'httpRequested': False}
            row['status'] = 'running'
            if failure == arm['cellId']:
                raise RuntimeError('synthetic-original-response-lost')
            row['httpCompleted'] = True

        def rebuild(arm, row):
            signal = pressure == arm['cellId']
            row.update(status='pressure-observed' if signal else 'diagnostic-complete', pressureObserved=signal,
                       originalHttpVerdict='quality-issues' if signal else 'descriptive-only',
                       cells=[{'caseId': c, 'state': 'drift-or-pressure-observed' if signal else 'unknown'} for c in g.CASES])

        op.run_arm, op.rebuild = run_arm, rebuild
        op.check_jobs = lambda _: None
        return op, calls

    def fake_linux(self):
        original = Path.read_text
        def text(path, *args, **kwargs):
            if str(path) == '/proc/sys/kernel/random/boot_id':
                return 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n'
            return original(path, *args, **kwargs)
        return (mock.patch.object(M.platform, 'system', return_value='Linux'),
                mock.patch.object(M, 'process_start', return_value={'state': 'R', 'startTicks': '123'}),
                mock.patch.object(Path, 'read_text', text))

    def test_pressure_finishes_cpu_pair_bars_higher_in_both_blocks_and_retains_36_cells(self):
        op, calls = self.run_fixture(pressure='b01-r20-cpu1')
        a, b, c = self.fake_linux()
        with a, b, c:
            result = op.run()
        self.assertIsNone(result['failure'])
        self.assertEqual(len(result['rows']), 12)
        self.assertEqual(len(result['cells']), 36)
        self.assertEqual(len(calls), 8)
        self.assertIn('b01-r20-cpu2', calls)
        self.assertIn('b02-r20-cpu1', calls)
        self.assertFalse(any('r40' in call for call in calls))
        self.assertEqual(result['barHigherRatesThan'], 20)
        self.assertEqual(sum(r['status'] == 'not-executed' for r in result['rows']), 4)
        self.assertFalse(result['performanceAccepted'])
        self.assertIsNone(result['driverHeadroomAccepted'])
        # The original plan is claimed globally; a second execution cannot dispatch.
        with a, b, c:
            with self.assertRaises(FileExistsError):
                op.run()
        self.assertEqual(len(calls), 8)

    def test_lost_original_response_stops_all_and_keeps_every_unexecuted_denominator(self):
        op, calls = self.run_fixture(failure='b01-r10-cpu1')
        a, b, c = self.fake_linux()
        with a, b, c:
            result = op.run()
        self.assertEqual(calls, ['b01-r10-cpu1'])
        self.assertIn('response-lost', result['failure'])
        self.assertEqual(result['rows'][0]['status'], 'invalid')
        self.assertEqual(sum(r['status'] == 'not-executed' for r in result['rows']), 11)
        self.assertEqual(len(result['cells']), 36)

    def cleanup_fixture(self):
        op = object.__new__(M.Operation)
        op.root = self.root
        op.b = M.fixed_budgets(M.generator())
        arm = copy.deepcopy(M.read(PLAN / 'sequence.json')['physicalArms'][0])
        op.current = {'arm': arm, 'targetRequested': True, 'driverRequested': True,
                      'httpRequested': True, 'targetId': 'a' * 64, 'driverId': 'd' * 64}
        op.cleanup_deadline = op.step_deadline = None
        calls, deadlines = [], []
        op.argv = lambda name: (['node', '--root', '/evidence/tasks', 'task', 'status', '--request-id', arm['requestId']]
                                if name == 'wait-original-http' else [name])
        def command(argv, *_args, **_kwargs):
            calls.append(argv)
            deadlines.append(op.cleanup_deadline)
            return {'exitCode': 0}
        def cli(argv, *_args, **_kwargs):
            command(argv)
            if '--request-id' in argv:
                raise RuntimeError('synthetic-status-response-lost')
            return {}
        values = iter([{'Id': 'd' * 64, 'State': {'Running': True, 'OOMKilled': False}},
                       {'Id': 'd' * 64, 'State': {'Running': False, 'OOMKilled': False, 'ExitCode': 143}}])
        op.driver_info = lambda **_: next(values)
        op.target_info = lambda **_: {'Id': 'a' * 64, 'State': {'Running': False, 'OOMKilled': False}}
        op.inspect = lambda *_args, **_kwargs: None
        op.wait_task = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('synthetic-target-query-lost'))
        op.command, op.cli = command, cli
        return op, calls, deadlines

    def test_reused_shared_cleanup_queries_original_only_and_does_not_skip_owned_stops(self):
        op, calls, deadlines = self.cleanup_fixture()
        row = {'issues': []}
        self.assertFalse(op.cleanup(row))
        names = [argv[0] for argv in calls]
        self.assertIn('stop-driver', names)
        self.assertIn('remove-owned-driver', names)
        self.assertIn('stop-target', names)
        self.assertIn('remove-owned-target', names)
        self.assertEqual(len(set(deadlines)), 1)
        self.assertNotIn('run-http', names)
        self.assertFalse(row['cleanupComplete'])
        self.assertTrue(row['driverRemoved'])
        self.assertTrue(row['targetRemoved'])

    def test_owned_driver_actual_cpu2_contract_rejects_cpu1_network_owner_and_mount_drift(self):
        sequence, runtime = M.read(PLAN / 'sequence.json'), M.read(PLAN / 'runtime-selection.json')
        arm = sequence['physicalArms'][1]
        op = object.__new__(M.Operation)
        op.current = {'arm': arm, 'targetId': 'a' * 64}
        op.spec = {'driver': runtime['driver'], 'owner': sequence['owner']}
        create = op.operation('create-driver')['argvTemplate']
        mounts = []
        for i, word in enumerate(create):
            if word == '--mount':
                value = create[i + 1]
                mounts.append({'Destination': value.split(',dst=', 1)[1].split(',')[0],
                               'Source': value.split(',src=', 1)[1].split(',dst=', 1)[0],
                               'RW': not value.endswith(',readonly')})
        info = {'Id': 'd' * 64, 'Name': '/' + arm['runId'] + '-driver', 'Image': runtime['driver']['image'],
                'Config': {'Labels': {'org.apache.hugegraph.hgbench.driver-headroom.request': arm['runId']},
                           'User': sequence['owner'], 'WorkingDir': '/source', 'Entrypoint': ['/usr/local/bin/python3'],
                           'Cmd': ['-B', '-c', 'import time; time.sleep(750)']},
                'HostConfig': {'NetworkMode': 'container:' + 'a' * 64, 'ReadonlyRootfs': True,
                               'Memory': 768 * M.MiB, 'MemorySwap': 768 * M.MiB, 'NanoCpus': 2000000000,
                               'PidsLimit': 128, 'CapDrop': ['ALL'], 'Privileged': False,
                               'SecurityOpt': ['no-new-privileges:true'], 'Init': True,
                               'RestartPolicy': {'Name': 'no'}, 'LogConfig': {'Type': 'none'},
                               'Tmpfs': {'/tmp': 'rw,nosuid,nodev,noexec,size=67108864'}}, 'Mounts': mounts}
        op.inspect = lambda *_args, **_kwargs: copy.deepcopy(info)
        self.assertEqual(op.driver_info()['Id'], 'd' * 64)
        for mutate, code in ((lambda i: i['HostConfig'].update(NanoCpus=1000000000), 'isolation'),
                             (lambda i: i['HostConfig'].update(NetworkMode='host'), 'isolation'),
                             (lambda i: i['Config']['Labels'].clear(), 'owner'),
                             (lambda i: i['Mounts'][0].update(RW=True), 'mount')):
            changed = copy.deepcopy(info)
            mutate(changed)
            op.inspect = lambda *_args, _value=changed, **_kwargs: _value
            with self.assertRaisesRegex(RuntimeError, code):
                op.driver_info()

    def test_cgroup_endpoints_conserve_cpu_throttle_and_refuse_reset_identity_and_overlap(self):
        before = {'containerId': 'd' * 64, 'identity': {'pid': 123, 'startTimeTicks': '456', 'bootId': 'boot',
                   'cgroupPath': '/owned', 'device': '1', 'inode': '2'}, 'control': {'quota': '100000'},
                  'beforeMonotonicNs': '1000000000', 'afterMonotonicNs': '1000001000',
                  'counters': {'usage_usec': '10', 'nr_throttled': '2', 'throttled_usec': '4'}}
        after = copy.deepcopy(before)
        after.update(beforeMonotonicNs='2000000000', afterMonotonicNs='2000001000',
                     counters={'usage_usec': '100010', 'nr_throttled': '3', 'throttled_usec': '9'})
        delta = M.Operation.endpoint_delta(before, after)
        self.assertEqual(delta['delta'], {'usage_usec': '100000', 'nr_throttled': '1', 'throttled_usec': '5'})
        self.assertLess(delta['averageCpuCoresRange'][0], .1)
        self.assertGreater(delta['averageCpuCoresRange'][1], .1)
        self.assertTrue(delta['throttledObserved'])
        for mutate in (lambda a: a['identity'].update(inode='3'), lambda a: a.update(containerId='b' * 64),
                       lambda a: a['counters'].update(usage_usec='0'),
                       lambda a: a.update(beforeMonotonicNs='1000000000')):
            bad = copy.deepcopy(after)
            mutate(bad)
            with self.assertRaises(RuntimeError):
                M.Operation.endpoint_delta(before, bad)

    def test_prepare_deadline_exhaustion_cannot_submit_and_recovery_operation_is_status_only(self):
        op = object.__new__(M.Operation)
        arm = M.read(PLAN / 'sequence.json')['physicalArms'][0]
        op.current = {'arm': arm, 'targetId': 'a' * 64, 'driverId': 'd' * 64}
        calls = []
        op.driver_info = lambda: {'State': {'Running': True}}
        op.cli = lambda argv, *_args: calls.append(argv)
        with self.assertRaisesRegex(RuntimeError, 'driver-prepare-deadline'):
            op.prepare_http(time.monotonic() - 1)
        self.assertFalse(any('run' in argv for argv in calls))
        self.assertIn('status', op.argv('wait-original-http'))
        self.assertNotIn('run', op.argv('wait-original-http'))

    def test_actual_fixed_run_arm_uses_one_foreground_cli_durable_identity_and_separate_prepare_budget(self):
        op = object.__new__(M.Operation)
        g = M.generator()
        op.root, op.plan = self.root, self.root
        op.b = M.fixed_budgets(g)
        op.spec = {'windowPolicy': g.WINDOW_POLICY}
        runtime = M.read(PLAN / 'runtime-selection.json')
        arm = g.sequence_arm(self.root, Path('/frozen-v6'), runtime, g.schedule()[0], 'a' * 16, True, '1000:1000')
        directory = Path(arm['directory'])
        (directory / 'environment/work/mode-pre').mkdir(parents=True)
        (directory / 'driver-evidence/http-plan').mkdir(parents=True)
        M.save(directory / 'environment/work/mode-pre/result.json', {'status': 'passed', 'observedMode': 'forward'})
        M.save(directory / 'driver-evidence/http-plan/task.source.json', {'timeoutMs': 609000, 'terminateGraceMs': 5000})
        M.save(directory / 'driver-evidence/http-plan/task.json', {'digest': 'b' * 64})
        op.common = types.SimpleNamespace(ActiveUsage=lambda *_: None)
        op.deadline = 100000
        op.command_count = 0
        op.cleanup_deadline = op.step_deadline = None
        calls, snapshots, clock = [], [], [1000.0]
        op.check_jobs = lambda _: None
        op.target_info = lambda: None
        def driver_info(**_):
            op.current['driverId'] = 'd' * 64
            return {'State': {'Running': True}}
        op.driver_info = driver_info
        op.checkpoint = lambda: snapshots.append(copy.deepcopy(op.current))
        op.cpu_endpoint = lambda label: {'label': label}
        op.endpoint_delta = lambda *_: {'fixture': True, 'performanceAccepted': False}

        def command(argv, timeout=None, **_):
            calls.append((list(argv), timeout, op.step_deadline))
            if 'start' in argv:
                clock[0] += 15  # The whole start command has its own budget.
            return {'exitCode': 0}

        def cli(argv, timeout=None, **_):
            command(argv, timeout)
            if 'environment' in argv and 'status' in argv:
                return {'ready': {'jcmdExecutable': '/opt/jdk/bin/jcmd', 'java': {'pid': 2}},
                        'state': 'running', 'container': {'id': 'a' * 64}}
            if 'plan' in argv:
                self.assertAlmostEqual(op.step_deadline - clock[0], 30)
                clock[0] += 29.9
            if 'task' in argv and 'run' in argv:
                request = M.K.flag(argv, '--request-id')
                if '--detach' in argv:
                    return {'taskId': M.K.task_id(request), 'reused': False}
                self.assertTrue(snapshots[-1]['httpRequested'])
                self.assertEqual(snapshots[-1]['httpPlanDigest'], 'b' * 64)
                self.assertAlmostEqual(timeout, 644000)
                return {'taskId': M.K.task_id(request), 'planDigest': 'b' * 64,
                        'terminal': True, 'state': 'completed', 'interrupted': False, 'reused': False}
            return {}

        op.command, op.cli = command, cli
        row = {'issues': []}
        with mock.patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]):
            op.run_arm(arm, row)
        foreground = [argv for argv, _, _ in calls if 'task' in argv and 'run' in argv and '--detach' not in argv]
        self.assertEqual(len(foreground), 1)
        self.assertFalse(any('task' in argv and 'status' in argv for argv, _, _ in calls))
        self.assertTrue(row['httpCompleted'])
        self.assertEqual(op.current['httpTaskId'], M.K.task_id(arm['requestId']))

    def test_endpoint_whole_deadline_clamps_actual_child_command_and_restores_outer_deadline(self):
        op = object.__new__(M.Operation)
        op.root = self.root
        (op.root / 'commands').mkdir()
        op.b = M.fixed_budgets(M.generator())
        op.common = M.K.load_module(M.generator().H.LOCAL_SOURCE / 'components/http/comparison-workflow.py',
                                   'headroom_real_command_fixture')
        op.environment = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
        op.command_count = 0
        op.deadline = time.monotonic() + 20
        op.cleanup_deadline = op.step_deadline = None
        op.guard = lambda: op.basic()
        # Real detached local child; deliberately requests 5s, beyond the endpoint's 3s.
        script = 'import os,time; print(os.getpid(),flush=True); time.sleep(30)'
        op.driver_info = lambda: op.command([sys.executable, '-B', '-c', script], 5000)
        began = time.monotonic()
        with self.assertRaises(Exception):
            op.cpu_endpoint('before')
        self.assertLess(time.monotonic() - began, 4.5)
        self.assertIsNone(op.step_deadline)
        receipt = M.read(op.root / 'commands/00001.json')
        elapsed = (int(receipt['finishedMonotonicNs']) - int(receipt['startedMonotonicNs'])) / 1e9
        self.assertGreater(elapsed, 2.8)
        self.assertLess(elapsed, 4)
        self.assertNotEqual(receipt['exitCode'], 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(receipt['stdout'].strip()), 0)

    def test_rebuild_preserves_pressure_and_rejects_other_quality_issues_wrong_answers_and_changed_bytes(self):
        # Protocol output fixtures only. The actual public analyzer/window component
        # has its own real-HTTP raw/lock tests; this verifies operation decisions.
        op = object.__new__(M.Operation)
        arm = copy.deepcopy(M.read(PLAN / 'sequence.json')['physicalArms'][0])
        arm['directory'] = str(self.root / 'arm')
        directory = Path(arm['directory'])
        raw = directory / 'driver-evidence/http-plan/output'
        (raw / 'analysis').mkdir(parents=True)
        (directory / 'rebuilt-http').mkdir()
        profile = M.read(PLAN / 'arms/b01-r10-cpu1/driver-evidence/profile.json')
        M.save(directory / 'driver-evidence/profile.json', profile)
        M.save(raw / 'implementation.json', {'nodeExecutableSha256': 'a' * 64})
        report = {'profile': profile, 'gates': {'correctness': 'passed'}, 'serviceChecks': [{}, {}],
                  'runs': [{'caseId': c, 'loadId': 'open-10', 'scheduled': 1200, 'correct': 1200,
                            'failed': 0, 'dropped': 0, 'late': 0, 'issues': ['measurement:driver-pressure-observed'],
                            'driver': {'averageCpuCores': .9}} for c in M.generator().CASES]}
        op.spec = {'nodeExecutable': {'sha256': 'a' * 64}}
        op.b = M.fixed_budgets(M.generator())
        op.current = {'arm': arm}
        op.cli = lambda *_args, **_kwargs: None

        def write_report(value):
            text = (json.dumps(value) + '\n').encode()
            (raw / 'analysis/report.json').write_bytes(text)
            (directory / 'rebuilt-http/report.json').write_bytes(text)

        write_report(report)
        # Pressure survives the quality checks and reaches the still-unsatisfied
        # lifecycle gate; it never becomes a performance/quality pass.
        with self.assertRaisesRegex(RuntimeError, 'normal-lifecycle-unproved'):
            op.rebuild(arm, {})
        for mutate, error in ((lambda r: r['gates'].update(correctness='failed'), 'correctness'),
                              (lambda r: r['runs'][0].update(failed=1), 'measurement-quality'),
                              (lambda r: r['runs'][0].update(issues=['warmup:request-budget-exhausted']), 'fatal-http-quality')):
            changed = copy.deepcopy(report)
            mutate(changed)
            write_report(changed)
            with self.assertRaisesRegex(RuntimeError, error):
                op.rebuild(arm, {})
        write_report(report)
        with (raw / 'analysis/report.json').open('ab') as stream:
            stream.write(b' ')
        with self.assertRaisesRegex(RuntimeError, 'http-rebuild-byte-difference'):
            op.rebuild(arm, {})

    def test_unit_round_shares_ten_seconds_across_queries_and_restores_outer_deadline(self):
        op = object.__new__(M.Operation)
        op.root = self.root
        op.b = M.fixed_budgets(M.generator())
        op.current = {'arm': {'runId': 'fixture'}}
        op.sequence = {'declaredInactiveUnits': ['old.service', 'other.service'],
                       'protectedObservedUnit': 'protected.service'}
        op.command_count = 0
        op.environment = {}
        op.cleanup_deadline = None
        op.step_deadline = 150
        op.deadline = 200
        clock, durations = [100.0], []
        op.guard = lambda: op.basic()

        def command(argv, seconds, *_args):
            durations.append(seconds)
            clock[0] += 6
            unit = argv[3]
            fields = {'Id': unit, 'LoadState': 'not-found', 'ActiveState': 'inactive', 'SubState': 'dead',
                      'MainPID': '0', 'ControlGroup': '', 'Result': 'success'}
            return {'exitCode': 0, 'stdout': '\n'.join(k + '=' + v for k, v in fields.items())}

        op.common = types.SimpleNamespace(command=command)
        with mock.patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(RuntimeError, 'command-time-budget'):
                op.check_jobs('pre')
        self.assertEqual(durations, [10, 4])
        self.assertEqual(op.step_deadline, 150)
        self.assertNotIn('declaredHeavyUnits', op.sequence)


if __name__ == '__main__':
    unittest.main()
