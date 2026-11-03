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

"""Finite local protocol fixtures for headroom002; no Docker, HTTP, SSH or scores.

Only the draft test invokes the frozen public offline planners. Container,
collector and lifecycle records below are explicitly synthetic protocol inputs.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
FILE = ROOT / '.hgbench/http-driver-headroom-002-execute.py'
SPEC = importlib.util.spec_from_file_location('headroom002_fixture', FILE)
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
G = M.generator()
DRAFT = ROOT / '.hgbench/http-driver-headroom-002-draft-v2'


class Headroom002(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hgbench-headroom002-')
        self.root = Path(self.temp.name)
        self.runtime = G.H.read(G.H.TEMPLATES / 'runtime-settings.json')
        self.runtime['hostPython'] = str(Path(sys.executable).resolve())
        self.source = G.H.LOCAL_SOURCE
        self.arm = G.sequence_arm(self.root, self.source, self.runtime, G.schedule()[0], 'a' * 16, True, '1000:1000')
        M.K.SIGNAL = None

    def tearDown(self):
        M.K.SIGNAL = None
        self.temp.cleanup()

    def op(self):
        op = object.__new__(M.Operation)
        op.root, op.plan = self.root, self.root
        op.b, op.limits = M.fixed_budgets(G), G.budgets()
        op.runtime = self.runtime
        op.host_boot = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
        op.spec = {'node': str(Path(__import__('shutil').which('node')).resolve()),
                   'sourceRoot': str(self.source), 'owner': '1000:1000',
                   'dockerExecutable': '/synthetic/docker', 'driver': self.runtime['driver']}
        op.current = {'arm': self.arm, 'collectorTaskId': M.K.task_id(self.arm['collectorRequestId']),
                      'targetRequested': True, 'driverRequested': True, 'httpRequested': False,
                      'collectorRequested': False, 'targetId': 'a' * 64, 'driverId': 'd' * 64}
        op.deadline = time.monotonic() + 26000
        op.arm_deadline = op.driver_deadline = op.cleanup_deadline = op.step_deadline = None
        op.start = time.monotonic()
        op.command_count = op.last_scan = op.combined_peak = 0
        op.seen_targets, op.seen_drivers = set(), set()
        op.usage, op.arm_usage = [], []
        op.guard = lambda: op.basic()
        op.checkpoint = lambda: None
        return op

    def test_budget_full_observation_and_original_thresholds_are_explicit(self):
        b = G.budgets()
        self.assertEqual((b['workflowDeadlineMs'], b['httpTaskTimeoutMs'], b['httpWaitMs']), (549000, 609000, 644000))
        self.assertEqual((b['driverLifetimeRequiredMs'], b['targetLifetimeRequiredMs'], b['collectorLifetimeRequiredMs']),
                         (855000, 1294000, 735000))
        self.assertEqual((b['jvmOriginalParseMaximumMs'], b['jvmIndependentParseMaximumMs']), (15000, 15000))
        self.assertEqual(b['operationMinimumMs'], 25248000)
        self.assertEqual(b['operationMaximumMs'], 26000000)
        self.assertLess(b['reservedRetainedBytesIncludingSeal'], 16 * M.GiB)
        self.assertEqual(b['parentRuntimeMaxSeconds'], 26100)
        self.assertEqual(b['operationCleanupMs'], 90000)
        self.assertEqual(b['stepMaximumMs']['postCleanWindow'], 10000)
        self.assertEqual(G.analysis_contract()['driverPressureSignals']['analyzerAbsoluteProcessCpuCores'], .8)
        self.assertEqual(G.analysis_contract()['warmupReview']['p50AndP95DescriptiveDifferenceBudget'], {'relative': .1, 'absoluteMs': 1})
        self.assertEqual(len(G.schedule()) * len(G.CASES), 36)
        self.assertIsNone(G.analysis_contract()['driverHeadroomAccepted'])
        self.assertIsNone(G.analysis_contract()['warmupStable'])

    def test_recipe_fixed_public_argv_and_observer_order_no_polling_during_http(self):
        for row in G.schedule():
            arm = G.sequence_arm(self.root, self.source, self.runtime, row, 'a' * 16, True, '1000:1000')
            operations = {entry['budgetKey']: entry for entry in arm['operations']}
            keys = list(operations)
            self.assertEqual(G.suite_settings('fixture', row['ratePerSecond'], [])['loads'][0]['maxInFlight'], 4)
            self.assertTrue(arm['collector'] and arm['jvmObservation'])
            self.assertLess(keys.index('firstSample'), keys.index('waitHttp'))
            self.assertLess(keys.index('verifyCollector'), keys.index('cleanupDriver'))
            self.assertLess(keys.index('parseJvmOriginal'), keys.index('parseJvmIndependent'))
            self.assertLess(keys.index('phaseWindows'), keys.index('postCleanWindow'))
            argv = operations['waitHttp']['argvTemplate']
            self.assertIn('run', argv)
            self.assertNotIn('--detach', argv)
            self.assertNotIn('status', argv)
            self.assertIn('status', operations['waitHttp']['recoveryOnlyArgvTemplate'])
            parser = operations['parseJvmOriginal']['argvTemplate']
            self.assertEqual(parser[0], self.runtime['hostPython'])
            self.assertTrue(parser[2].endswith('/environment/jvm-observation.py'))
            self.assertEqual(parser[3:5], ['--json', 'parse'])
            create = operations['createDriver']['argvTemplate']
            self.assertEqual(create[create.index('--cpus') + 1], str(row['driverCpus']))
            self.assertEqual(create[-1], 'import time; time.sleep(900)')

    def test_current_inactivity_including_gc_does_not_prove_history(self):
        fields = {'Id': 'old.service', 'LoadState': 'not-found', 'ActiveState': 'inactive', 'SubState': 'dead',
                  'MainPID': '0', 'ControlGroup': '', 'Result': 'success'}
        def raw(value):
            return '\n'.join(k + '=' + v for k, v in value.items()) + '\n'
        for load in ('loaded', 'not-found'):
            actual = G.job_snapshot('old.service', raw(dict(fields, LoadState=load)))
            self.assertFalse(actual['priorTaskSuccessProven'])
        for key, value in (('Id', 'wrong.service'), ('LoadState', 'unknown'), ('ActiveState', 'active'),
                           ('SubState', 'running'), ('MainPID', '1'), ('ControlGroup', '/live')):
            with self.assertRaises(ValueError):
                G.job_snapshot('old.service', raw(dict(fields, **{key: value})))
        with self.assertRaises(ValueError):
            G.job_snapshot('old.service', raw(fields) + 'Id=old.service\n')

    def test_actual_frozen_draft_public_plans_and_resealed_override_fail_closed(self):
        output = self.root / 'execution'
        digest = M.sha(M.raw_file(DRAFT / 'plan.lock.json'))
        M.freeze(DRAFT, digest, output)
        op = M.Operation(output)
        self.assertFalse(op.spec['runnable'])
        self.assertEqual(len(op.spec['expectedTasks']), 12)
        ids = [row[k] for row in op.spec['expectedTasks'] for k in ('httpTaskId', 'targetTaskId', 'collectorTaskId')]
        self.assertEqual(len(set(ids)), 36)
        self.assertEqual(op.spec['observationStratum'], {'collector': True, 'jvmObservation': True, 'maxInFlight': 4})
        with self.assertRaisesRegex(RuntimeError, 'prepared-linux-owner-required'):
            op.run()
        self.assertFalse((output / 'dispatch.json').exists())
        changed = M.read(output / 'execution-spec.json')
        changed['budgets']['cleanupMs'] += 1
        M.save(output / 'execution-spec.json', changed, True)
        M.save(output / 'execution.lock.json', {'kind': 'http-driver-headroom-execution-lock',
               'specSha256': M.sha(M.raw_file(output / 'execution-spec.json'))}, True)
        with self.assertRaisesRegex(RuntimeError, 'execution-runtime-or-fixed-spec-differs'):
            M.load_execution(output)

    def collector_fixture(self, cpu=1, wrong_first=False):
        op = self.op()
        op.current['arm']['driverCpus'] = cpu
        directory = Path(self.arm['directory'])
        directory.mkdir(parents=True)
        token = 'f' * 32
        M.save(directory / 'collector-binding-template.json', G.collector_template(directory, token))
        records = {role: {'cgroupRaw': '0::/owned/' + role + '\n'} for role in ('target', 'driver')}
        identities = {role: {'pid': index + 100, 'bootId': op.host_boot, 'startTimeTicks': '123'}
                      for index, role in enumerate(('target', 'driver'))}
        sample = {'index': 0, 'schemaVersion': 1, 'startedMonotonicNs': '90', 'finishedMonotonicNs': '100',
                  'roles': [{'role': role, 'identity': copy.deepcopy(identities[role])} for role in ('target', 'driver')]}
        if wrong_first:
            sample['roles'][1]['identity']['startTimeTicks'] = 'changed'
        op.anchors = lambda: (records, identities)
        component_sha = M.sha(M.raw_file(self.source / 'components/http/resource-collector.py'))
        marker = {}
        op.common = types.SimpleNamespace(first_sample=lambda _: sample,
             publish_marker=lambda path, value: marker.update(value),
             verify_collector=lambda *_: {'sourceOrigin': 'linux-procfs', 'controlledStop': {'request': marker},
                                          'componentSha256': component_sha})
        op.collector = M.K.load_module(self.source / 'components/http/resource-collector.py', 'headroom002_collector_fixture')
        calls = []
        def cli(argv, *_args, **_kwargs):
            calls.append(list(map(str, argv)))
            if 'plan' in argv:
                expected = G.collector_source(directory, self.source, self.runtime['hostPython'], True)
                canonical = {**expected, 'command': [str(Path(expected['command'][0]).resolve()), *expected['command'][1:]],
                             'cwd': str(Path(expected['cwd']).resolve()),
                             'trackedFiles': [str(Path(p).resolve()) for p in expected['trackedFiles']]}
                M.save(directory / 'collector-task.json', {'digest': 'b' * 64, 'plan': canonical})
                return {}
            return {'taskId': op.current['collectorTaskId'], 'planDigest': 'b' * 64, 'reused': False}
        op.cli = cli
        return op, directory, calls, identities

    def test_collector_binds_public_plan_then_first_complete_sample_before_http(self):
        op, directory, calls, identities = self.collector_fixture()
        row = {'issues': []}
        op.bind_collector(row)
        self.assertEqual(op.current['firstSampleFinishedNs'], '100')
        self.assertEqual(op.current['anchorIdentities'], identities)
        self.assertTrue(op.current['collectorRequested'])
        self.assertFalse(op.current['httpRequested'])
        self.assertEqual(sum('run' in argv for argv in calls), 1)
        self.assertIn('--detach', calls[-1])
        self.assertEqual(M.read(directory / 'collector-spec.json')['roles'],
                         [{'role': 'target', 'hostPid': 100}, {'role': 'driver', 'hostPid': 101}])

    def test_wrong_first_sample_identity_cannot_continue_to_http(self):
        op, _, calls, _ = self.collector_fixture(wrong_first=True)
        with self.assertRaisesRegex(RuntimeError, 'first-sample-anchor-differs'):
            op.bind_collector({'issues': []})
        self.assertFalse(op.current['httpRequested'])
        self.assertEqual(sum('run' in argv for argv in calls), 1)

    def test_collector_cpu2_context_independent_verification_precedes_anchor_cleanup(self):
        op, directory, calls, _ = self.collector_fixture(cpu=2)
        row = {'issues': []}
        op.bind_collector(row)
        (directory / 'collector').mkdir()
        M.save(directory / 'collector/spec.json', op.current['collectorSpec'])
        op.current.update(httpStartedNs='150', httpFinishedNs='250')
        op.wait_original = lambda *_: {'terminal': True, 'state': 'completed', 'interrupted': False}
        def command(argv, *_args, **_kwargs):
            context = M.read(directory / 'collector-verification-context.json')
            self.assertEqual(context['expectedRoles']['driver']['limits']['cpus'], 2)
            self.assertEqual(context['expectedRoles']['target']['limits']['cpus'], 2)
            self.assertEqual(context['workflowWindow'], ['150', '250'])
            self.assertLessEqual(op.step_deadline - time.monotonic(), 30)
            return {'stdout': json.dumps({'status': 'verified', 'sourceOrigin': 'linux-procfs', 'qualityIssues': []})}
        op.command = command
        op.finish_collector(row)
        self.assertTrue(row['collectorCompleted'] and row['collectorBranchVerified'])
        self.assertFalse(row.get('driverRemoved', False))

    def test_query_cancel_failures_still_clean_both_owned_containers_with_one_90s_deadline(self):
        op = self.op()
        op.current.update(collectorRequested=True, httpRequested=True)
        commands, deadlines = [], []
        op.args = lambda key, index=0: [key, str(index)]
        def command(argv, *_args, **_kwargs):
            commands.append(argv)
            deadlines.append(op.cleanup_deadline)
            return {'exitCode': 0}
        def bad_status(role, *_args, **_kwargs):
            commands.append([role, 'query-or-cancel'])
            raise RuntimeError('synthetic-status-lost')
        op.command, op.task_status = command, bad_status
        op.cli = lambda argv, *_args, **_kwargs: command(argv)
        op.wait_original = lambda role, *_: bad_status(role)
        values = iter([{'Id': 'd' * 64, 'State': {'Running': True}},
                       {'Id': 'd' * 64, 'State': {'Running': False}}])
        op.driver_info = lambda *_: next(values)
        op.target_info = lambda *_: {'Id': 'a' * 64, 'State': {'Running': False}}
        op.inspect = lambda *_: None
        began = time.monotonic()
        row = {'issues': []}
        self.assertFalse(op.cleanup(row))
        self.assertTrue(row['driverRemoved'] and row['targetRemoved'])
        self.assertEqual(len(set(deadlines)), 1)
        self.assertGreater(deadlines[0], began + 89)
        self.assertLess(deadlines[0], began + 91)
        self.assertTrue(any(argv == ['cleanupDriver', '0'] for argv in commands))
        self.assertTrue(any(argv == ['removeTarget', '0'] for argv in commands))
        self.assertTrue(any('collector-original' in issue for issue in row['issues']))
        self.assertFalse(any('run' in argv for argv in commands))
        self.assertIsNone(op.cleanup_deadline)

    def fixed_run_fixture(self):
        op = self.op()
        directory = Path(self.arm['directory'])
        (directory / 'environment/work/mode-pre').mkdir(parents=True)
        (directory / 'driver-evidence/http-plan').mkdir(parents=True)
        M.save(op.plan / 'source-selection.lock.json', {'synthetic': True})
        M.save(directory / 'environment/task.json', {'digest': 'c' * 64})
        M.save(directory / 'environment/work/mode-pre/result.json', {'status': 'passed', 'observedMode': 'forward'})
        task = {'timeoutMs': 609000, 'terminateGraceMs': 5000}
        M.save(directory / 'driver-evidence/http-plan/task.source.json', task)
        M.save(directory / 'driver-evidence/http-plan/task.json', {'digest': 'b' * 64, 'plan': task})
        op.common = types.SimpleNamespace(ActiveUsage=lambda *_: None)
        calls, events, snapshots = [], [], []
        op.checkpoint = lambda: snapshots.append(copy.deepcopy(op.current))
        op.check_jobs = lambda phase: events.append(phase)
        op.target_info = lambda: None
        def driver_info(*_):
            op.current['driverId'] = 'd' * 64
            return {'State': {'Running': True}}
        op.driver_info = driver_info
        op.command = lambda argv, *_args, **_kwargs: calls.append(list(map(str, argv)))
        def cli(argv, *_args, **_kwargs):
            calls.append(list(map(str, argv)))
            if 'environment' in argv and 'status' in argv:
                return {'ready': {'jcmdExecutable': '/opt/jdk/bin/jcmd', 'java': {'pid': 2}},
                        'state': 'running', 'container': {'id': 'a' * 64}}
            if 'task' in argv and 'run' in argv:
                role = 'target' if '--detach' in argv else 'http'
                if role == 'http':
                    self.assertTrue(snapshots[-1]['httpRequested'])
                    self.assertEqual(snapshots[-1]['httpPlanDigest'], 'b' * 64)
                    self.assertEqual(events[-1], 'before')
                    events.append('http')
                return {'taskId': op.current[role + 'TaskId'], 'planDigest': ('c' if role == 'target' else 'b') * 64,
                        'reused': False, 'terminal': True, 'state': 'completed', 'interrupted': False}
            return {}
        op.cli = cli
        def bind(_):
            events.append('first-sample')
            op.current['firstSampleFinishedNs'] = str(time.monotonic_ns())
        op.bind_collector = bind
        op.cpu_endpoint = lambda label: events.append(label)
        op.endpoint_delta = lambda *_: {'fixture': True}
        op.finish_collector = lambda row: events.append('collector-verified')
        op.normal_stop = lambda row: events.append('anchors-cleaned')
        return op, calls, events, snapshots

    def test_real_fixed_run_arm_dispatch_once_durable_intent_and_observer_order(self):
        op, calls, events, _ = self.fixed_run_fixture()
        row = {'issues': []}
        with mock.patch.object(G.H, 'locked_source', return_value={}):
            with mock.patch.object(M, 'generator', return_value=G):
                op.run_arm(self.arm, row)
        self.assertEqual(len([argv for argv in calls if 'task' in argv and 'run' in argv and '--detach' not in argv]), 1)
        self.assertTrue(row['httpCompleted'])
        self.assertLess(events.index('first-sample'), events.index('http'))
        self.assertLess(events.index('collector-verified'), events.index('anchors-cleaned'))
        self.assertIsNone(op.step_deadline)
        self.assertIn('httpFinishedNs', op.current)

    def test_lost_foreground_response_preserves_original_identity_and_restores_deadline(self):
        op, calls, _, snapshots = self.fixed_run_fixture()
        original = op.cli
        def cli(argv, *args, **kwargs):
            if 'task' in argv and 'run' in argv and '--detach' not in argv:
                calls.append(list(map(str, argv)))
                self.assertTrue(snapshots[-1]['httpRequested'])
                self.assertEqual(snapshots[-1]['httpTaskId'], M.K.task_id(self.arm['requestId']))
                raise RuntimeError('synthetic-foreground-response-lost')
            return original(argv, *args, **kwargs)
        op.cli = cli
        row = {'issues': []}
        with mock.patch.object(G.H, 'locked_source', return_value={}):
            with mock.patch.object(M, 'generator', return_value=G):
                with self.assertRaisesRegex(RuntimeError, 'foreground-response-lost'):
                    op.run_arm(self.arm, row)
        self.assertEqual(len([argv for argv in calls if 'task' in argv and 'run' in argv and '--detach' not in argv]), 1)
        self.assertTrue(op.current['httpRequested'])
        self.assertEqual(op.current['httpTaskId'], M.K.task_id(self.arm['requestId']))
        self.assertIn('httpFinishedNs', op.current)
        self.assertFalse(row.get('httpCompleted', False))
        self.assertIsNone(op.step_deadline)

    def run_fixture(self, pressure=None, failure=None):
        op = self.op()
        (self.root / 'prepared').mkdir()
        op.plan = self.root / 'prepared'
        node = self.root / 'synthetic-node'
        node.write_bytes(b'synthetic-executable-not-run')
        M.save(self.root / 'execution-spec.json', {'synthetic': True})
        op.spec.update(runnable=True, owner='%d:%d' % (os.getuid(), os.getgid()), node=str(node),
                       nodeExecutable={'sha256': M.sha(node.read_bytes())})
        op.policy = G.analysis_contract()
        arms = [G.sequence_arm(self.root / 'prepared', self.source, self.runtime, row, 'a' * 16, True, op.spec['owner'])
                for row in G.schedule()]
        op.sequence = {'physicalArms': arms}
        op.rows = [{**{k: arm[k] for k in ('runId', 'blockId', 'cellId', 'ratePerSecond', 'driverCpus')},
                    'status': 'not-started', 'issues': [], 'cells': []} for arm in arms]
        op.current, op.barred_above = None, None
        called = []
        def run_arm(arm, row):
            called.append(arm['cellId'])
            op.current = {'arm': arm}
            row.update(status='running', cleanupComplete=True)
            if arm['cellId'] == failure:
                raise RuntimeError('synthetic-original-response-lost')
            row['httpCompleted'] = True
        def rebuild(arm, row):
            observed = arm['cellId'] == pressure
            row.update(status='pressure-observed' if observed else 'diagnostic-complete', pressureObserved=observed,
                       cells=[{'caseId': case, 'state': 'unknown'} for case in G.CASES])
        op.run_arm, op.rebuild = run_arm, rebuild
        op.check_jobs = lambda *_: None
        op.checkpoint = lambda: None
        return op, called

    def fake_linux(self):
        original = Path.read_text
        def text(path, *args, **kwargs):
            return ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n' if str(path) == '/proc/sys/kernel/random/boot_id'
                    else original(path, *args, **kwargs))
        return (mock.patch.object(M.platform, 'system', return_value='Linux'),
                mock.patch.object(M, 'process_start', return_value={'state': 'R', 'startTicks': '123'}),
                mock.patch.object(Path, 'read_text', text))

    def test_pressure_retains_matching_cpu_member_bars_higher_rates_and_keeps_36_cells(self):
        op, called = self.run_fixture(pressure='b01-r20-cpu1')
        a, b, c = self.fake_linux()
        with a, b, c:
            result = op.run()
        self.assertIsNone(result['failure'])
        self.assertEqual(len(result['rows']), 12)
        self.assertEqual(len(result['cells']), 36)
        self.assertEqual(len(called), 8)
        self.assertIn('b01-r20-cpu2', called)
        self.assertIn('b02-r20-cpu1', called)
        self.assertFalse(any('r40' in name for name in called))
        self.assertFalse(result['performanceAccepted'])
        self.assertIsNone(result['driverHeadroomAccepted'])
        with a, b, c:
            with self.assertRaises(FileExistsError):
                op.run()
        self.assertEqual(len(called), 8)

    def test_lost_original_response_stops_following_arms_without_losing_denominator(self):
        op, called = self.run_fixture(failure='b01-r10-cpu1')
        a, b, c = self.fake_linux()
        with a, b, c:
            result = op.run()
        self.assertIn('response-lost', result['failure'])
        self.assertEqual(called, ['b01-r10-cpu1'])
        self.assertEqual(len(result['cells']), 36)
        self.assertEqual(result['rows'][0]['status'], 'invalid')
        self.assertEqual(sum(row['status'] == 'not-executed' for row in result['rows']), 11)

    def test_independent_jvm_requires_complete_recomputed_capture_and_actual_context(self):
        op = self.op()
        directory = Path(self.arm['directory'])
        (directory / 'environment').mkdir(parents=True)
        M.save(directory / 'environment/plan.json', {'synthetic': True})
        op.current['ready'] = {'java': {'pid': 2, 'startTimeTicks': '300'}}
        op.settings = G.H.read(G.H.TEMPLATES / 'settings.json')
        calls = []
        proof = {'evidenceIntegrity': 'passed', 'parserRecomputed': True, 'captureComplete': True,
                 'rawComplete': True, 'observationStatus': 'completed'}
        def command(argv):
            calls.append(list(map(str, argv)))
            if 'jvm-evidence.mjs' in str(argv):
                context = M.read(directory / 'jvm-verification-context.json')
                self.assertEqual(context['settings'], op.settings)
                self.assertEqual(context['processIdentity']['pid'], 2)
                self.assertEqual(context['hostBootId'], op.host_boot)
            return {'stdout': json.dumps(proof)}
        op.jvm_command = command
        row = {'issues': []}
        op.verify_jvm(row)
        self.assertTrue(row['jvmBranchVerified'])
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0][2].endswith('/environment/jvm-observation.py'))
        (directory / 'jvm-verification-context.json').unlink()
        (directory / 'jvm-verified.json').unlink()
        proof['rawComplete'] = False
        with self.assertRaisesRegex(RuntimeError, 'jvm-independent-evidence-failed'):
            op.verify_jvm({'issues': []})
        self.assertFalse((directory / 'jvm-verified.json').exists())

    def test_jvm_cancel_allows_fixed_owner_to_reap_its_detached_parser_before_outer_kill(self):
        op = self.op()
        (self.root / 'commands').mkdir()
        op.environment = dict(os.environ)
        op.arm_deadline = op.step_deadline = time.monotonic() + 15
        marker = self.root / 'owner-cleaned.json'
        code = ('import json,os,signal,subprocess,sys,time\n'
                'child=subprocess.Popen([sys.executable,"-B","-c","import time; time.sleep(60)"],start_new_session=True)\n'
                'def stop(s,f):\n'
                ' os.killpg(child.pid,signal.SIGKILL); child.wait(); open(sys.argv[1],"w").write(json.dumps({"child":child.pid,"reaped":True})); sys.exit(143)\n'
                'signal.signal(signal.SIGTERM,stop)\n'
                'print(os.getpid(),flush=True)\n'
                'time.sleep(60)\n')
        begun = time.monotonic()
        def guard():
            if time.monotonic() - begun > .3:
                raise RuntimeError('synthetic-cancel')
        op.guard = guard
        with self.assertRaisesRegex(RuntimeError, 'synthetic-cancel'):
            op.jvm_command([sys.executable, '-B', '-c', code, str(marker)])
        self.assertLess(time.monotonic() - begun, 4)
        proof = M.read(marker)
        receipt = M.read(self.root / 'commands/00001.json')
        self.assertTrue(proof['reaped'])
        self.assertTrue(receipt['ownedGroupCleanup']['termSent'])
        self.assertTrue(receipt['ownedGroupCleanup']['leaderReaped'])
        with self.assertRaises(ProcessLookupError):
            os.kill(proof['child'], 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(receipt['stdout']), 0)

    def test_jvm_output_cap_is_fail_closed_and_reaped(self):
        op = self.op()
        (self.root / 'commands').mkdir()
        op.environment = dict(os.environ)
        op.b['maxCommandBytes'] = 256
        op.arm_deadline = op.step_deadline = time.monotonic() + 15
        with self.assertRaisesRegex(RuntimeError, 'jvm-command-output-budget'):
            op.jvm_command([sys.executable, '-B', '-c', 'print("a"*1024,flush=True)'])
        receipt = M.read(self.root / 'commands/00001.json')
        self.assertLessEqual(len(receipt['stdout'].encode()) + len(receipt['stderr'].encode()), 256)
        self.assertTrue(receipt['ownedGroupCleanup']['leaderReaped'])


if __name__ == '__main__':
    unittest.main()
