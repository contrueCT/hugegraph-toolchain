# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to You under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Explicit local protocol fixtures, plus a real public task/cancel worker. No Docker/HTTP."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / 'components/http/capacity-workflow.py'
SPEC = importlib.util.spec_from_file_location('http_capacity_workflow_test', COMPONENT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def execution_runtime():
    return {'schemaVersion': 1, 'kind': 'http-capacity-execution-runtime',
            'cleanWindow': {'systemctlExecutable': '/usr/bin/systemctl', 'requireClean': True,
                            'units': ['retained-convert.service']},
            'budgets': {'prepareMilliseconds': 60000, 'commandMilliseconds': 1000,
                        'maxCommandBytes': MOD.MiB, 'maxWorkBytes': 128 * MOD.MiB,
                        'minFreeBytes': 0, 'cleanupMilliseconds': 20000}}


class CapacityWorkflow(unittest.TestCase):
    def test_go_capacity_requires_all_copied_helpers_and_preserves_sdk_source_selection(self):
        shared = self.runtime['comparisonRuntime']
        shared.update(schemaVersion=2, executor={'engine': 'go-http-v1', 'clockProof': '/fixture/clock',
            'binary': {'path': '/fixture/runner', 'sha256': 'a' * 64, 'sourceSha256': 'b' * 64,
                       'goVersion': 'go1.27.3', 'goos': 'linux', 'goarch': 'amd64'}})
        self.assertIn('release/http-go-toolchain.json', MOD.COMMON.source_selection(shared))
        extra = ('go-comparison-runtime.py', 'go-process-observer.py', 'go-clock-probe.py')
        for name in extra:
            shutil.copyfile(COMPONENT.with_name(name), self.root / name)
        for name in ('comparison-runtime.json', 'capacity/project/runtime.json'):
            MOD.save(self.root / name, shared, replace=True)
        plan = MOD.read_json(self.root / 'workflow.json')
        plan['runtime'] = self.runtime
        plan['controlFiles'] = [MOD.entry(self.root / name, name) for name in MOD.CONTROLS + extra]
        MOD.save(self.root / 'workflow.json', plan, replace=True)
        lock = MOD.read_json(self.root / 'workflow.lock.json')
        lock['planSha256'] = MOD.entry(self.root / 'workflow.json')['sha256']
        MOD.save(self.root / 'workflow.lock.json', lock, replace=True)
        MOD.load(self.root, verify=True)
        (self.root / 'go-process-observer.py').write_text('changed fixture')
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'control-changed'):
            MOD.load(self.root, verify=True)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.calls = []
        self.uid = patch.object(MOD.os, 'geteuid', return_value=1000)
        self.uid.start()
        self.addCleanup(self.uid.stop)
        MOD.CANCELLED = False
        MOD.CANCELLED_AT = None
        self.runtime = execution_runtime()
        self.node = Path(subprocess.check_output(['node', '-p', 'process.execPath'], text=True).strip()).resolve()
        shared = {'schemaVersion': 1, 'kind': 'http-comparison-runtime', 'sourceRoot': str(ROOT),
                  'nodeHome': str(self.node.parent.parent), 'hostPython': str(Path(sys.executable).resolve()),
                  'dockerExecutable': '/usr/bin/docker',
                  'driver': {'image': 'sha256:' + 'a' * 64, 'python': '/usr/bin/python3', 'cpus': 1,
                             'memoryBytes': 512 * MOD.MiB, 'pids': 128, 'tmpfsBytes': 64 * MOD.MiB,
                             'wallSeconds': 600, 'shutdownSeconds': 1, 'maxLogBytes': MOD.MiB},
                  'collector': {'intervalMs': 100, 'durationMs': 300000, 'maxSamples': 3001,
                                'maxOutputBytes': MOD.MiB},
                  'supervisor': {'pollMs': 100, 'commandMs': 1000, 'maxCommandBytes': MOD.MiB,
                                 'minFreeBytes': 0, 'maxWorkBytes': 64 * MOD.MiB}}
        self.runtime['comparisonRuntime'] = shared
        for name in ('capacity', 'capacity/project', 'children', 'observations', 'prepare-commands'):
            (self.root / name).mkdir()
        for name in MOD.CONTROLS[:3]:
            shutil.copyfile(COMPONENT.with_name(name), self.root / name)
        MOD.save(self.root / 'comparison-runtime.json', shared)
        shutil.copyfile(self.root / 'comparison-runtime.json', self.root / 'capacity/project/runtime.json')
        MOD.save(self.root / 'execution-runtime.json', {k: v for k, v in self.runtime.items() if k != 'comparisonRuntime'})
        attempt = '1' * 32
        self.points = []
        for index in range(11):
            identity = 'point-' + format(index, '032x')
            request = 'capacity:' + attempt + ':' + identity
            self.points.append({'id': identity, 'index': index, 'caseId': 'fixture-' + str(index),
                'mode': 'forward', 'ratePerSecond': 10, 'comparisonDirectory': 'points/' + identity + '/comparison',
                'comparisonPlanDigest': 'a' * 64, 'comparisonContentDigest': 'b' * 64,
                'directory': str(self.root / 'children' / identity), 'requestId': request,
                'taskId': MOD.COHORT.task_id(request)})
            self.schedule(self.points[-1])
        self.capacity = {'kind': 'http-capacity-plan', 'planDigest': 'c' * 64, 'phase': 'exploration',
            'estimate': {'safetyUpperBoundMilliseconds': 300000},
            'policy': {'pointBudgets': {'maxTotalMilliseconds': 1000},
                       'controllerBudgets': {'perPointMilliseconds': 1000},
                       'studyBudgets': {'maxEvidenceBytes': 128 * MOD.MiB, 'maxPlanBytes': 16 * MOD.MiB}},
                         'points': self.points}
        MOD.save(self.root / 'capacity/capacity-plan.json', self.capacity)
        plan = {'schemaVersion': 1, 'kind': 'http-capacity-workflow-plan', 'attemptId': attempt,
            'root': str(self.root), 'owner': {'uid': 1000, 'gid': os.getegid()}, 'runtime': self.runtime,
            'capacityPlanDigest': 'c' * 64, 'capacityContentDigest': 'd' * 64, 'points': self.points,
            'taskRoot': str(self.root / 'task-state'), 'requiredJobs': self.runtime['cleanWindow']['units'],
            'controlFiles': [MOD.entry(self.root / name, name) for name in MOD.CONTROLS],
            'identities': {'source': [], 'node': {}, 'python': {}}, 'maxRunMilliseconds': 300000,
            'budgetSemantics': 'local protocol fixture, not verified benchmark evidence',
            'replayAllowed': False, 'engineeringAccepted': False, 'performanceAccepted': False}
        MOD.save(self.root / 'workflow.json', plan)
        MOD.save(self.root / 'workflow.lock.json', {'schemaVersion': 1, 'kind': 'http-capacity-workflow-lock',
            'attemptId': attempt, 'planSha256': MOD.entry(self.root / 'workflow.json')['sha256']})

    def schedule(self, point, single=False, budget_mode='stage-sum'):
        """Only local controller protocol data; the public JS verifier is not bypassed in production."""
        folder = self.root / 'capacity' / point['comparisonDirectory']
        folder.mkdir(parents=True, exist_ok=True)
        count = 1 if single else 4
        arms = [{'arm': 'O' if single else ('A' if i % 2 == 0 else 'B'), 'position': i,
                 'runId': 'arm-' + MOD.COMMON.sha((point['id'] + ':' + str(i)).encode())[:40],
                 'expectedMode': point['mode']} for i in range(count)]
        value = {'schemaVersion': 3 if single else 1, 'planDigest': point['comparisonPlanDigest'],
                 'comparison': 'single-observation' if single else 'aa',
                 'budgets': {'armCount': count},
                 'variants': {key: {'mode': point['mode']} for key in (['O'] if single else ['A', 'B'])},
                 'blocks': ([{'id': 'observe-01', 'order': 'O', 'arms': arms}] if single else
                            [{'id': 'explore-' + str(i + 1), 'order': 'AB', 'arms': arms[2 * i:2 * i + 2]}
                             for i in range(2)])}
        if single:
            value.update(design='single-observation')
            value['budgets']['budgetMode'] = budget_mode
        MOD.save(folder / 'plan.json', value, replace=(folder / 'plan.json').exists())
        MOD.save(folder / 'plan.lock.json', {'contentDigest': point['comparisonContentDigest']},
                 replace=(folder / 'plan.lock.json').exists())
        return value

    def single_fixture(self, budget_mode='total'):
        self.capacity['schemaVersion'] = 2
        self.capacity['policy'].update(schemaVersion=2, budgetMode=budget_mode,
            exploration={'mode': 'single-run', 'runs': 1, 'warmupMs': 0, 'durationMs': 100})
        declared = 240000 if budget_mode == 'total' else 1200000
        self.capacity['policy']['studyBudgets']['maxTotalMilliseconds'] = declared
        self.capacity['estimate']['safetyUpperBoundMilliseconds'] = 900000
        self.capacity.update(budgetMode=budget_mode,
            totalDeadlineMilliseconds=declared, cleanupReserveMilliseconds=100000,
            minimumBudgetMilliseconds=161100 if budget_mode == 'total' else 900000)
        for point in self.points:
            point['expectedArms'] = 1
            self.schedule(point, single=True, budget_mode=budget_mode)
        MOD.save(self.root / 'capacity/capacity-plan.json', self.capacity, replace=True)
        workflow = MOD.read_json(self.root / 'workflow.json')
        workflow.update(points=self.points, maxRunMilliseconds=240000 if budget_mode == 'total' else 900000)
        MOD.save(self.root / 'workflow.json', workflow, replace=True)
        MOD.save(self.root / 'workflow.lock.json', {'schemaVersion': 1, 'kind': 'http-capacity-workflow-lock',
            'attemptId': '1' * 32, 'planSha256': MOD.entry(self.root / 'workflow.json')['sha256']}, replace=True)

    def confirmation_fixture(self, count=2):
        """Explicit local protocol fixture, never a source-report or runtime verification claim."""
        self.points = self.points[:count]
        for index, point in enumerate(self.points):
            point.pop('mode')
            point['id'] = 'confirm-' + format(index, '032x')
            point.update(rateIndex=0, expectedArms=8, comparison='forward-embedded' if index == 0 else 'aa',
                         modes={'A': 'forward' if index == 0 else 'embedded', 'B': 'embedded'},
                         sourcePoints=[{'mode': mode, 'pointId': 'point-' + format(index, '032x'),
                                        'relation': 'candidate'}
                                       for mode in (['forward', 'embedded'] if index == 0 else ['embedded'])])
            point.update(directory=str(self.root / 'children' / point['id']),
                         comparisonDirectory='points/' + point['id'] + '/comparison',
                         requestId='capacity:' + '1' * 32 + ':' + point['id'])
            point['taskId'] = MOD.COHORT.task_id(point['requestId'])
        self.capacity.update(kind='http-capacity-confirmation-plan', phase='confirmation', points=self.points,
                             modes=['forward', 'embedded'],
                             budgets={'point': {'maxTotalMilliseconds': 1100},
                                      'controller': {'perPointMilliseconds': 1200, 'totalMilliseconds': 220000},
                                      'study': {'maxEvidenceBytes': 128 * MOD.MiB, 'maxPlanBytes': 16 * MOD.MiB}},
                             selection={'selections': [
                                 {'caseId': 'fixture-' + str(i), 'mode': mode,
                                  'status': 'provisional-candidate' if i == 0 else 'unknown', 'confirmed': False}
                                 for i in range(11) for mode in ['forward', 'embedded']]})
        self.capacity['estimate']['sourceReviewMaximumMilliseconds'] = 1000
        (self.root / 'capacity/capacity-plan.json').unlink()
        MOD.save(self.root / 'capacity/confirmation-plan.json', self.capacity)
        workflow = MOD.read_json(self.root / 'workflow.json')
        workflow['points'] = self.points
        MOD.save(self.root / 'workflow.json', workflow, replace=True)
        MOD.save(self.root / 'workflow.lock.json', {'schemaVersion': 1, 'kind': 'http-capacity-workflow-lock',
            'attemptId': '1' * 32, 'planSha256': MOD.entry(self.root / 'workflow.json')['sha256']}, replace=True)

    def controller(self, state='completed', lost=False):
        controller = MOD.Controller(self.root)
        controller.source_current = lambda: None  # Explicit fixture: no provenance gate is claimed.
        controller.clean = lambda name: self.calls.append(['clean', name])
        def prepare(point):
            folder = Path(point['directory'])
            folder.mkdir()
            (folder / 'execution').mkdir()
            MOD.save(folder / 'task.json', {'digest': 'e' * 64, 'plan': {'timeoutMs': 1000}})
            MOD.save(folder / 'runtime.lock.json', {'fixture': True})
            runtime_digest = MOD.entry(folder / 'runtime.lock.json')['sha256']
            MOD.save(folder / 'result.json', {'kind': 'http-comparison-workflow-result', 'ok': True,
                'executionCompleted': True, 'evidenceValid': True})
            MOD.save(folder / 'execution/terminal.json', {'status': 'completed', 'runtimeReverified': True,
                'failure': None, 'planDigest': point['comparisonPlanDigest'], 'runtimeDigest': runtime_digest})
            MOD.save(folder / 'execution/execution.json', {'planDigest': point['comparisonPlanDigest'],
                'runtimeDigest': runtime_digest})
            MOD.save(folder / 'execution/execution.lock.json', {'planDigest': point['comparisonPlanDigest']})
            return {**point, 'taskPlan': str(folder / 'task.json'), 'taskPlanDigest': 'e' * 64,
                    'taskPlanSha256': MOD.entry(folder / 'task.json')['sha256'], 'taskTimeoutMs': 1000,
                    'runtimeDigest': runtime_digest}
        controller.prepare_point = prepare
        def cli(args, seconds=10, record=None, failed_reply=False, long=False):
            self.calls.append(list(map(str, args)))
            if args[:3] == ['http', 'capacity', 'verify'] or args[:4] == ['http', 'capacity', 'confirmation', 'verify']:
                return {'valid': True, 'planDigest': 'c' * 64, 'contentDigest': 'd' * 64, 'plan': self.capacity}
            point = next(p for p in self.points if p['requestId'] in args)
            self.assertNotIn('--detach', args)
            self.assertTrue(long)
            self.assertTrue((self.root / 'observations' / (point['id'] + '-dispatch.json')).is_file())
            self.assertEqual(MOD.read_json(self.root / 'current.json')['child']['taskId'], point['taskId'])
            if lost:
                raise MOD.COMMON.WorkflowError('fixture-lost-response')
            return {'taskId': point['taskId'], 'planDigest': 'e' * 64, 'terminal': True,
                    'interrupted': False, 'state': state, 'reused': False}, 0 if state == 'completed' else 6
        controller.cli = cli
        controller.stop_child = lambda child: {'fixture': True, 'taskId': child['taskId'], 'cleanupProven': False}
        return controller

    def run_controller(self, controller):
        with patch.object(MOD.platform, 'system', return_value='Linux'):
            return controller.run()

    def test_runtime_forbids_overrides_unknown_fields_and_unbounded_cleanup(self):
        value = {k: v for k, v in self.runtime.items() if k != 'comparisonRuntime'}
        MOD.validate_runtime(value)
        for changed in ({**value, 'comparisonRuntime': {}}, {**value, 'extra': True},
                        {**value, 'budgets': {**value['budgets'], 'cleanupMilliseconds': 90000}}):
            with self.assertRaises(MOD.COMMON.WorkflowError):
                MOD.validate_runtime(changed)

    def test_single_observation_keeps_eleven_points_and_dispatches_each_original_id_once(self):
        self.single_fixture()
        result = self.run_controller(self.controller())
        self.assertTrue(result['ok'])
        self.assertEqual(result['explorationMode'], 'single-run')
        self.assertEqual(result['plannedArms'], 11)
        self.assertEqual(result['budgetMode'], 'total')
        self.assertEqual(result['totalDeadlineMilliseconds'], 240000)
        self.assertEqual(len(result['points']), 11)
        self.assertTrue(all(row['expectedArms'] == 1 and row['state'] == 'completed' for row in result['points']))
        self.assertEqual(sum('run' in row for row in self.calls), 11)
        self.assertFalse(result['statisticsCertified'])
        self.assertFalse(result['performanceAccepted'])

    def test_single_lost_response_never_repeats_and_keeps_full_planned_denominator(self):
        self.single_fixture()
        result = self.run_controller(self.controller(lost=True))
        self.assertFalse(result['ok'])
        self.assertEqual(result['points'][0]['state'], 'submission-response-unknown')
        self.assertEqual(result['points'][0]['taskId'], self.points[0]['taskId'])
        self.assertTrue(all(row['state'] == 'not-started' and row['expectedArms'] == 1
                            for row in result['points'][1:]))
        self.assertEqual(result['plannedArms'], 11)
        self.assertEqual(sum('run' in row for row in self.calls), 1)
        self.assertFalse(result['cleanup']['cleanupProven'])

    def test_single_policy_requires_version_two_and_exact_one_planned_arm(self):
        self.single_fixture()
        workflow = MOD.read_json(self.root / 'workflow.json')
        workflow['points'][0]['expectedArms'] = 4
        MOD.save(self.root / 'workflow.json', workflow, replace=True)
        lock = MOD.read_json(self.root / 'workflow.lock.json')
        lock['planSha256'] = MOD.entry(self.root / 'workflow.json')['sha256']
        MOD.save(self.root / 'workflow.lock.json', lock, replace=True)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'single-observation-point-shape'):
            MOD.load(self.root)
        self.capacity['policy']['schemaVersion'] = 1
        MOD.save(self.root / 'capacity/capacity-plan.json', self.capacity, replace=True)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'requires-policy-two'):
            MOD.study(self.root / 'capacity')
        self.assertFalse((self.root / 'dispatch.json').exists())

    def test_single_schedule_has_only_original_o_run_and_refuses_copied_b_or_foreign_runtime_mode(self):
        self.single_fixture()
        controller = MOD.Controller(self.root)
        point = self.points[0]
        original = MOD.read_json(self.root / 'capacity' / point['comparisonDirectory'] / 'plan.json')
        schedule = controller.point_schedule(point, original)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0]['arm'], 'O')
        for changed in ('duplicate', 'wrong-arm', 'wrong-budget-mode'):
            value = json.loads(json.dumps(original))
            if changed == 'duplicate':
                value['blocks'][0]['arms'].append(dict(value['blocks'][0]['arms'][0]))
            elif changed == 'wrong-arm':
                value['blocks'][0]['arms'][0]['arm'] = 'B'
            else:
                value['budgets']['budgetMode'] = 'stage-sum'
            with self.subTest(changed=changed):
                MOD.save(self.root / 'capacity' / point['comparisonDirectory'] / 'plan.json', value, replace=True)
                with self.assertRaises(MOD.COMMON.WorkflowError):
                    controller.point_schedule(point, value)
        MOD.save(self.root / 'capacity' / point['comparisonDirectory'] / 'plan.json', original, replace=True)
        copied = json.loads(json.dumps(original))
        copied['blocks'][0]['arms'][0]['runId'] = 'arm-' + 'f' * 40
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'schedule-drift'):
            controller.point_schedule(point, copied)

    def test_single_prepared_child_matches_original_one_arm_schedule_and_runtime(self):
        self.single_fixture()
        controller = MOD.Controller(self.root)
        controller.plan['identities']['node'] = {'bytes': 1, 'sha256': 'f' * 64}
        point = self.points[0]
        extra_arm = False
        def prepare(args, *unused, **kwargs):
            self.assertEqual(args[:3], ['http', 'comparison', 'prepare'])
            folder = Path(point['directory'])
            (folder / 'comparison').mkdir(parents=True)
            child_plan = MOD.read_json(self.root / 'capacity' / point['comparisonDirectory'] / 'plan.json')
            if extra_arm:
                child_plan['blocks'][0]['arms'].append({**child_plan['blocks'][0]['arms'][0], 'arm': 'B'})
            MOD.save(folder / 'comparison/plan.json', child_plan)
            MOD.save(folder / 'comparison/plan.lock.json', {'contentDigest': point['comparisonContentDigest']})
            MOD.save(folder / 'task.json', {'digest': 'e' * 64, 'plan': {'timeoutMs': 2000}})
            MOD.save(folder / 'runtime.json', {'planDigest': point['comparisonPlanDigest'],
                'planContentDigest': point['comparisonContentDigest'], 'settings': self.runtime['comparisonRuntime'],
                'identities': {'source': [], 'hostPython': {}, 'node': [
                    {'path': 'bin/node', 'type': 'file', 'bytes': 1, 'sha256': 'f' * 64}]}})
            MOD.save(folder / 'runtime.lock.json', {'fixture': True})
            return {'attempt': str(folder), 'executed': False, 'replayAllowed': False,
                    'taskPlan': str(folder / 'task.json'), 'taskPlanDigest': 'e' * 64,
                    'runtimeDigest': MOD.entry(folder / 'runtime.lock.json')['sha256']}
        controller.cli = prepare
        child = controller.prepare_point(point)
        self.assertEqual(child['expectedArms'], 1)
        self.assertEqual(child['taskId'], point['taskId'])
        self.assertEqual(child['taskTimeoutMs'], 2000)
        shutil.rmtree(point['directory'])
        extra_arm = True
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'schedule-drift'):
            controller.prepare_point(point)
        self.assertFalse((self.root / 'dispatch.json').exists())

    def test_total_uses_declared_study_deadline_and_clamps_actual_foreground_to_remaining_active_time(self):
        self.single_fixture()
        controller = MOD.Controller(self.root)
        self.assertEqual(controller.plan['maxRunMilliseconds'], 240000)
        self.assertGreater(self.capacity['estimate']['safetyUpperBoundMilliseconds'], 240000)
        self.assertAlmostEqual(controller.hard_deadline - controller.active_deadline, 100, places=6)
        controller.active_deadline = time.monotonic() + .2
        controller.step_deadline = time.monotonic() + 100
        with patch.object(MOD, 'invoke', return_value={}) as invoke:
            controller.cli(['task', 'run'], 999, long=True)
        self.assertGreater(invoke.call_args.args[2], 0)
        self.assertLessEqual(invoke.call_args.args[2], .2)

    def test_total_deadline_exhausted_during_prepare_never_records_or_dispatches_child(self):
        self.single_fixture()
        controller = self.controller()
        original = controller.prepare_point
        def prepare(point):
            child = original(point)
            controller.active_deadline = time.monotonic() - .001
            return child
        controller.prepare_point = prepare
        result = self.run_controller(controller)
        self.assertEqual(result['failure'], 'capacity-study-deadline')
        self.assertTrue(all(row['state'] == 'not-started' for row in result['points'][1:]))
        self.assertFalse(any('run' in row for row in self.calls))
        self.assertFalse((self.root / 'current.json').exists())

    def test_total_cleanup_shares_parent_hard_deadline_and_keeps_unknown_absence(self):
        self.single_fixture()
        controller = self.controller()
        child = controller.prepare_point(self.points[0])
        controller.stop_child = MOD.Controller.stop_child.__get__(controller)
        controller.hard_deadline = time.monotonic() + 1
        def cli(args, *unused, **kwargs):
            if 'stop' in args:
                return {'stopRequested': True, 'errors': []}
            return {'taskId': child['taskId'], 'planDigest': child['taskPlanDigest'],
                    'terminal': True, 'interrupted': False, 'state': 'cancelled'}
        controller.cli = cli
        result = controller.stop_child(child)
        self.assertEqual(controller.cleanup_deadline, controller.hard_deadline)
        self.assertTrue(result['taskTerminalProven'])
        self.assertFalse(result['cleanupProven'])

    def test_single_stage_sum_keeps_conservative_sum_and_confirmation_policy_two_keeps_eight_arms(self):
        self.single_fixture('stage-sum')
        info = MOD.study(self.root / 'capacity')
        self.assertEqual(info['runMilliseconds'], 900000)
        self.assertEqual(info['plan']['totalDeadlineMilliseconds'], 1200000)
        self.assertFalse(MOD.Controller(self.root).total_budget)
        self.capacity['policy']['studyBudgets']['maxTotalMilliseconds'] = 899999
        self.capacity['totalDeadlineMilliseconds'] = 899999
        MOD.save(self.root / 'capacity/capacity-plan.json', self.capacity, replace=True)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'declared-total-budget'):
            MOD.study(self.root / 'capacity')
        self.capacity['policy']['studyBudgets']['maxTotalMilliseconds'] = 1200000
        self.capacity['totalDeadlineMilliseconds'] = 1200000
        MOD.save(self.root / 'capacity/capacity-plan.json', self.capacity, replace=True)
        self.confirmation_fixture(count=1)
        self.capacity.update(schemaVersion=2, budgetMode='total', totalDeadlineMilliseconds=240000,
                             minimumBudgetMilliseconds=161100)
        self.capacity['budgets']['study']['maxTotalMilliseconds'] = 240000
        MOD.save(self.root / 'capacity/confirmation-plan.json', self.capacity, replace=True)
        workflow = MOD.read_json(self.root / 'workflow.json')
        workflow['maxRunMilliseconds'] = 240000
        MOD.save(self.root / 'workflow.json', workflow, replace=True)
        lock = MOD.read_json(self.root / 'workflow.lock.json')
        lock['planSha256'] = MOD.entry(self.root / 'workflow.json')['sha256']
        MOD.save(self.root / 'workflow.lock.json', lock, replace=True)
        info = MOD.study(self.root / 'capacity')
        self.assertFalse(info['singleObservation'])
        self.assertEqual(MOD.Controller(self.root).plan['points'][0]['expectedArms'], 8)

    def test_confirmation_uses_original_two_children_and_full_twenty_two_selection_rows(self):
        self.confirmation_fixture()
        controller = self.controller()
        result = self.run_controller(controller)
        self.assertTrue(result['ok'])
        self.assertEqual(result['phase'], 'confirmation')
        self.assertEqual(result['selectionRows'], 22)
        self.assertEqual(result['selections'], self.capacity['selection']['selections'])
        self.assertEqual(result['plannedComparisons'], 2)
        self.assertFalse(result['confirmationQualified'])
        self.assertEqual([r['state'] for r in result['points']], ['completed', 'completed'])
        self.assertTrue(all(r['expectedArms'] == 8 for r in result['points']))
        commands = [c for c in self.calls if c[:4] == ['http', 'capacity', 'confirmation', 'verify']]
        self.assertEqual(len(commands), 2)
        self.assertEqual(sum('run' in c for c in self.calls), 2)
        mapping = MOD.read_json(self.root / 'execution-set.json')
        self.assertEqual(mapping['kind'], 'http-capacity-execution-set')
        self.assertEqual(mapping['planDigest'], self.capacity['planDigest'])
        self.assertEqual([r['id'] for r in mapping['points']], [r['id'] for r in self.points])

    def test_confirmation_lost_response_never_resubmits_and_keeps_missing_selection_rows(self):
        self.confirmation_fixture()
        result = self.run_controller(self.controller(lost=True))
        self.assertFalse(result['ok'])
        self.assertEqual(result['points'][0]['state'], 'submission-response-unknown')
        self.assertEqual(result['points'][1]['state'], 'not-started')
        self.assertEqual(result['selectionRows'], 22)
        self.assertEqual(sum('run' in c for c in self.calls), 1)
        self.assertEqual(result['cleanup']['taskId'], self.points[0]['taskId'])
        self.assertFalse(result['cleanup']['cleanupProven'])

    def test_confirmation_post_source_replay_failure_preserves_completed_children_without_qualification(self):
        self.confirmation_fixture()
        controller = self.controller()
        original = controller.cli
        count = 0
        def cli(args, *rest, **kwargs):
            nonlocal count
            if args[:4] == ['http', 'capacity', 'confirmation', 'verify']:
                count += 1
                if count == 2:
                    raise MOD.COMMON.WorkflowError('fixture-original-report-changed')
            return original(args, *rest, **kwargs)
        controller.cli = cli
        result = self.run_controller(controller)
        self.assertFalse(result['ok'])
        self.assertEqual(result['failure'], 'fixture-original-report-changed')
        self.assertTrue(all(row['state'] == 'completed' for row in result['points']))
        self.assertFalse(result['confirmationQualified'])
        self.assertEqual(result['selectionRows'], 22)

    def test_confirmation_source_review_is_reserved_before_prepare_and_twice_in_global_allowance(self):
        self.confirmation_fixture()
        info = MOD.study(self.root / 'capacity')
        MOD.confirmation_review_budget(info, self.runtime)
        self.runtime['budgets']['prepareMilliseconds'] = 10999
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'confirmation-prepare-review-budget'):
            MOD.confirmation_review_budget(info, self.runtime)
        self.runtime['budgets']['prepareMilliseconds'] = 11000
        info['budgets']['controller']['totalMilliseconds'] = 121999
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'confirmation-controller-review-budget'):
            MOD.confirmation_review_budget(info, self.runtime)
        info['budgets']['controller']['totalMilliseconds'] = 122000
        MOD.confirmation_review_budget(info, self.runtime)

    def test_confirmation_zero_children_rejects_prepare_before_any_mutation_or_command(self):
        self.confirmation_fixture(count=0)
        output = self.root / 'never-created'
        with patch.object(MOD, 'invoke', side_effect=AssertionError('no public command for zero children')):
            with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'confirmation-no-eligible-children'):
                MOD.freeze(self.root / 'execution-runtime.json', self.root / 'capacity', output)
        self.assertFalse(output.exists())
        self.assertEqual(len(self.capacity['selection']['selections']), 22)

    def test_confirmation_wrong_physical_arm_count_and_ambiguous_plan_kind_reject_before_dispatch(self):
        self.confirmation_fixture()
        workflow = MOD.read_json(self.root / 'workflow.json')
        workflow['points'][0]['expectedArms'] = 4
        MOD.save(self.root / 'workflow.json', workflow, replace=True)
        lock = MOD.read_json(self.root / 'workflow.lock.json')
        lock['planSha256'] = MOD.entry(self.root / 'workflow.json')['sha256']
        MOD.save(self.root / 'workflow.lock.json', lock, replace=True)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'confirmation-point-shape'):
            MOD.load(self.root)
        MOD.save(self.root / 'capacity/capacity-plan.json', {'phase': 'exploration'})
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'one-plan-kind-required'):
            MOD.study(self.root / 'capacity')
        self.assertFalse((self.root / 'dispatch.json').exists())

    def test_confirmation_prepared_child_must_retain_all_eight_arms_and_original_modes(self):
        self.confirmation_fixture(count=1)
        controller = MOD.Controller(self.root)
        controller.plan['identities']['node'] = {'bytes': 1, 'sha256': 'f' * 64}
        point = self.points[0]
        arm_count = 8
        modes = dict(point['modes'])
        def prepare(args, *unused, **kwargs):
            folder = Path(point['directory'])
            (folder / 'comparison').mkdir(parents=True)
            MOD.save(folder / 'task.json', {'digest': 'e' * 64, 'plan': {'timeoutMs': 2000}})
            MOD.save(folder / 'comparison/plan.json', {
                'planDigest': point['comparisonPlanDigest'], 'budgets': {'armCount': arm_count},
                'blocks': [{'arms': [{}, {}]} for _ in range(4)], 'comparison': point['comparison'],
                'variants': {key: {'mode': mode} for key, mode in modes.items()}})
            MOD.save(folder / 'comparison/plan.lock.json', {'contentDigest': point['comparisonContentDigest']})
            MOD.save(folder / 'runtime.json', {
                'planDigest': point['comparisonPlanDigest'], 'planContentDigest': point['comparisonContentDigest'],
                'settings': self.runtime['comparisonRuntime'], 'identities': {
                    'source': [], 'hostPython': {},
                    'node': [{'path': 'bin/node', 'type': 'file', 'bytes': 1, 'sha256': 'f' * 64}]}})
            MOD.save(folder / 'runtime.lock.json', {'fixture': True})
            return {'attempt': str(folder), 'executed': False, 'replayAllowed': False,
                    'taskPlan': str(folder / 'task.json'), 'taskPlanDigest': 'e' * 64,
                    'runtimeDigest': MOD.entry(folder / 'runtime.lock.json')['sha256']}
        controller.cli = prepare
        child = controller.prepare_point(point)
        self.assertEqual(child['expectedArms'], 8)
        self.assertEqual(child['modes'], point['modes'])
        shutil.rmtree(point['directory'])
        arm_count = 4
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'confirmation-arms-or-modes'):
            controller.prepare_point(point)
        shutil.rmtree(point['directory'])
        arm_count = 8
        modes['B'] = 'forward'
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'confirmation-arms-or-modes'):
            controller.prepare_point(point)

    def test_confirmation_attempt_cannot_mutate_original_execution_or_parent_alias(self):
        self.confirmation_fixture()
        original = self.root / 'original-execution'
        original.mkdir()
        alias = self.root / 'original-alias'
        alias.symlink_to(original, target_is_directory=True)
        execution_set = self.root / 'original-execution-set.json'
        MOD.save(execution_set, {'points': [{'executionDirectory': 'original-alias'}]})
        self.capacity['exploration'] = {
            'executionSet': str(execution_set),
            'executionSetSha256': MOD.COMMON.sha(execution_set.read_bytes()),
            'planDirectory': str(self.root / 'original-plan'),
            'reportDirectory': str(self.root / 'original-report')}
        info = {'phase': 'confirmation', 'plan': self.capacity}
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'output-in-source-evidence'):
            MOD.confirmation_inputs_separate(info, (alias / 'new-attempt').resolve())
        MOD.confirmation_inputs_separate(info, self.root / 'separate-new-attempt')
        self.assertEqual(list(original.iterdir()), [])
        execution_set.write_text('{}')
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'source-set-changed'):
            MOD.confirmation_inputs_separate(info, self.root / 'separate-new-attempt')

    def test_confirmation_replay_owner_term_reaps_real_detached_grandchild_groups(self):
        # Local process protocol only: no source proof, Docker, HTTP or benchmark task is fabricated.
        chain = self.root / 'local-replay-chain.py'
        ready = self.root / 'nested-ready.json'
        leaf_ready = self.root / 'leaf-ready'
        code = '''
import json,os,pathlib,signal,subprocess,sys,time
root=pathlib.Path(ROOT)
mode=sys.argv[1]
if mode=='leaf':
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    (root/'leaf-ready').write_text(str(os.getpid()))
    time.sleep(60)
else:
    child=subprocess.Popen([sys.executable,'-B',__file__,'leaf' if mode=='middle' else 'middle'],start_new_session=True)
    def stop(signum,frame):
        os.killpg(child.pid,signal.SIGTERM)
        if mode=='middle':
            time.sleep(.2)
            os.killpg(child.pid,signal.SIGKILL)
        child.wait(timeout=3)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM,stop)
    while not (root/'leaf-ready').exists():time.sleep(.01)
    if mode=='leader':
        (root/'nested-ready.json').write_text(json.dumps({'leader':os.getpid(),'middle':child.pid,'leaf':int((root/'leaf-ready').read_text())}))
    time.sleep(60)
'''.replace('ROOT', repr(str(self.root)))
        chain.write_text(code)
        owner_code = '''
import importlib.util,json,signal,sys
spec=importlib.util.spec_from_file_location('capacity_replay_owner_fixture',COMPONENT)
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
signal.signal(signal.SIGTERM,mod.cancelled)
def guard():mod.need(not mod.CANCELLED,'fixture-owner-cancelled')
try:
    mod.confirmation_command([sys.executable,'-B',CHAIN,'leader'],30,65536,{},guard)
except mod.COMMON.WorkflowError as error:
    print(json.dumps({'error':str(error),'parentDeadline':mod.cancellation_deadline()}))
'''.replace('COMPONENT', repr(str(COMPONENT))).replace('CHAIN', repr(str(chain)))
        owner = subprocess.Popen([sys.executable, '-B', '-c', owner_code],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pids = None
        try:
            deadline = time.monotonic() + 5
            while not ready.exists() and owner.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(ready.exists())
            pids = json.loads(ready.read_text())
            self.assertTrue(leaf_ready.exists())
            started = time.monotonic()
            owner.send_signal(signal.SIGTERM)
            stdout, stderr = owner.communicate(timeout=5)
            self.assertEqual(owner.returncode, 0, stderr)
            self.assertEqual(json.loads(stdout)['error'], 'fixture-owner-cancelled')
            self.assertLess(time.monotonic() - started, 5)
            for pid in pids.values():
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.communicate(timeout=3)
            for pid in (pids or {}).values():
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_confirmation_replay_grace_and_cleanup_share_first_parent_term_deadline(self):
        controller = self.controller()
        child = controller.prepare_point(self.points[0])
        controller.stop_child = MOD.Controller.stop_child.__get__(controller)
        MOD.CANCELLED_AT = time.monotonic() - 28
        original_deadline = MOD.CANCELLED_AT + 29
        def cli(args, *unused, **kwargs):
            if 'stop' in args:
                return {'stopRequested': True, 'errors': []}
            return {'taskId': child['taskId'], 'planDigest': child['taskPlanDigest'],
                    'terminal': True, 'interrupted': False, 'state': 'cancelled'}
        controller.cli = cli
        result = controller.stop_child(child)
        self.assertEqual(controller.cleanup_deadline, original_deadline)
        self.assertTrue(result['taskTerminalProven'])
        self.assertFalse(result['cleanupProven'])
        self.assertLessEqual(controller.cleanup_deadline - time.monotonic(), 1)

    def test_once_dispatch_and_complete_denominator_without_detached_or_repeated_run(self):
        controller = self.controller()
        result = self.run_controller(controller)
        self.assertTrue(result['ok'])
        self.assertEqual(len(result['points']), 11)
        self.assertTrue(all(row['state'] == 'completed' for row in result['points']))
        self.assertEqual(len([row for row in self.calls if 'run' in row]), 11)
        original = (self.root / 'result.json').read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_controller(controller)
        self.assertEqual((self.root / 'result.json').read_bytes(), original)
        mapping = MOD.read_json(self.root / 'execution-set.json')
        self.assertEqual(mapping['planDigest'], 'c' * 64)
        self.assertEqual([row['id'] for row in mapping['points']], [point['id'] for point in self.points])
        self.assertFalse(result['engineeringAccepted'])

    def test_lost_response_never_retries_and_keeps_unstarted_denominator(self):
        result = self.run_controller(self.controller(lost=True))
        self.assertFalse(result['ok'])
        self.assertEqual(result['points'][0]['state'], 'submission-response-unknown')
        self.assertEqual([row['state'] for row in result['points'][1:]], ['not-started'] * 10)
        runs = [args for args in self.calls if 'run' in args]
        self.assertEqual(len(runs), 1)
        self.assertEqual(result['cleanup']['taskId'], self.points[0]['taskId'])

    def test_failed_lifecycle_is_not_a_capacity_boundary_and_stops_all_remaining_points(self):
        result = self.run_controller(self.controller(state='failed'))
        self.assertFalse(result['ok'])
        self.assertEqual(result['failure'], 'capacity-point-failed-no-boundary-claim')
        self.assertEqual(result['points'][0]['state'], 'failed')
        self.assertEqual(result['points'][1]['state'], 'not-started')
        self.assertEqual(result['boundaryClassification'], 'none; any failure stops the full study')

    def test_preexisting_stop_preserves_all_unstarted_rows_and_claims_attempt_once(self):
        controller = self.controller()
        MOD.save(self.root / 'stop-request.json', {'attemptId': controller.plan['attemptId'], 'planSha256': controller.sha})
        result = self.run_controller(controller)
        self.assertFalse(result['ok'])
        self.assertEqual(result['failure'], 'capacity-stop-requested')
        self.assertTrue(all(row['state'] == 'not-started' for row in result['points']))
        self.assertFalse(any('run' in row for row in self.calls))

    def test_source_failure_stops_before_prepare_without_running_changed_cli(self):
        controller = self.controller()
        controller.source_current = lambda: MOD.need(False, 'capacity-source-changed')
        result = self.run_controller(controller)
        self.assertEqual(result['failure'], 'capacity-source-changed')
        self.assertFalse(any('run' in row for row in self.calls))
        self.assertTrue(all(row['state'] == 'not-started' for row in result['points']))

    def test_source_changes_during_prepare_prevent_the_sole_submission(self):
        controller = self.controller()
        original = controller.prepare_point
        changed = [False]
        def prepare(point):
            child = original(point)
            changed[0] = True
            return child
        controller.prepare_point = prepare
        controller.source_current = lambda: MOD.need(not changed[0], 'capacity-source-changed')
        result = self.run_controller(controller)
        self.assertEqual(result['failure'], 'capacity-source-changed')
        self.assertEqual(result['points'][0]['state'], 'prepared')
        self.assertEqual(result['points'][1]['state'], 'not-started')
        self.assertFalse(any('run' in row for row in self.calls))
        self.assertFalse((self.root / 'current.json').exists())

    def test_deadline_failure_preserves_full_denominator(self):
        controller = self.controller()
        controller.started = time.monotonic() - 1000
        result = self.run_controller(controller)
        self.assertEqual(result['failure'], 'capacity-study-deadline')
        self.assertTrue(all(row['state'] == 'not-started' for row in result['points']))

    def test_work_limit_failure_preserves_full_denominator(self):
        controller = self.controller()
        controller.active_usage.byte_budget = 1
        result = self.run_controller(controller)
        self.assertEqual(result['failure'], 'capacity-work-budget')
        self.assertTrue(all(row['state'] == 'not-started' for row in result['points']))
        self.assertFalse(any('run' in row for row in self.calls))

    def test_cleanup_uses_only_original_identity_and_retains_errors_without_false_proof(self):
        controller = self.controller()
        child = controller.prepare_point(self.points[0])
        controller.stop_child = MOD.Controller.stop_child.__get__(controller)
        def cli(args, seconds=10, record=None, *unused, **keywords):
            self.calls.append(args)
            if 'cancel' in args:
                raise MOD.COMMON.WorkflowError('fixture-cancel-failed')
            if 'stop' in args:
                return {'stopRequested': True, 'errors': ['fixture-cleanup-unknown']}
            self.assertEqual(args[-1], child['taskId'])
            return {'taskId': child['taskId'], 'planDigest': child['taskPlanDigest'],
                    'terminal': True, 'interrupted': False, 'state': 'failed'}
        controller.cli = cli
        result = controller.stop_child(child)
        self.assertTrue(result['taskTerminalProven'])
        self.assertFalse(result['cleanupProven'])
        self.assertIn('fixture-cancel-failed', result['errors'])
        self.assertTrue(any('stop' in args and child['directory'] in args for args in self.calls))
        self.assertTrue(any('cancel' in args and child['taskId'] in args for args in self.calls))
        self.assertFalse(any('run' in args for args in self.calls))

    def test_cleanup_refuses_changed_runtime_and_does_not_execute_foreign_cli(self):
        controller = self.controller()
        child = controller.prepare_point(self.points[0])
        controller.stop_child = MOD.Controller.stop_child.__get__(controller)
        controller.source_current = lambda: MOD.need(False, 'capacity-source-changed')
        controller.cli = lambda *args, **keywords: self.fail('changed CLI must not run')
        result = controller.stop_child(child)
        self.assertFalse(result['cleanupProven'])
        self.assertFalse(result['taskTerminalProven'])

    def test_short_commands_clamp_but_foreground_has_its_own_task_deadline(self):
        controller = self.controller()
        controller.cli = MOD.Controller.cli.__get__(controller)
        received = []
        with patch.object(MOD, 'invoke', side_effect=lambda runtime, args, seconds, *rest: received.append(seconds)):
            controller.cli(['status'], 30)
            controller.cli(['run'], 30, long=True)
        self.assertEqual(received, [1, 30])

    def test_cleanup_expired_cannot_start_more_commands(self):
        controller = self.controller()
        controller.cli = MOD.Controller.cli.__get__(controller)
        controller.cleanup = True
        controller.cleanup_deadline = time.monotonic() - 1
        with patch.object(MOD, 'invoke', side_effect=AssertionError('no command after cleanup deadline')):
            with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'cleanup-deadline'):
                controller.cli(['status'])

    def test_saved_runtime_bytes_and_controller_hash_drift_are_rejected(self):
        MOD.save(self.root / 'comparison-runtime.json', {'changed': True}, replace=True)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'saved-runtime-drift'):
            MOD.load(self.root, verify=True)

    def test_frozen_helper_drift_is_rejected_before_dispatch(self):
        with (self.root / 'capacity-workflow.py').open('a') as stream:
            stream.write('\n# changed fixture\n')
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'control-changed'):
            MOD.load(self.root, verify=True)
        self.assertFalse((self.root / 'dispatch.json').exists())

    def test_public_task_is_cancelled_by_original_identity_and_real_worker_exits(self):
        # This is a real local public task. Its fixed Python fixture sleeps; it has no database effects.
        source = self.root / 'local-task-source.json'
        script = self.root / 'local-worker.py'
        worker_pid = self.root / 'worker-pid'
        script.write_text('import os,time,pathlib\npathlib.Path(' + repr(str(worker_pid)) + ').write_text(str(os.getpid()))\ntime.sleep(60)\n')
        MOD.save(source, {'schemaVersion': 1, 'kind': 'component-task', 'name': 'capacity-local-cancel-fixture',
            'command': [str(Path(sys.executable).resolve()), '-B', str(script)], 'cwd': str(self.root),
            'trackedFiles': [str(script)], 'effect': 'filesystem', 'timeoutMs': 65000,
            'terminateGraceMs': 500, 'maxLogBytes': 65536})
        frozen = self.root / 'local-task.json'
        argv = [str(self.node), str(ROOT / 'dist/cli.js')]
        reply = subprocess.run(argv + ['task', 'plan', str(source), '--out', str(frozen)],
                               capture_output=True, text=True, timeout=10, check=True)
        self.assertTrue(json.loads(reply.stdout)['ok'])
        controller = self.controller()
        point = self.points[0]
        task = MOD.read_json(frozen)
        child = {**point, 'taskPlan': str(frozen), 'taskPlanDigest': task['digest']}
        result = subprocess.run(argv + ['--root', controller.plan['taskRoot'], 'task', 'run', '--plan', str(frozen),
                                       '--request-id', child['requestId'], '--detach'], capture_output=True,
                                text=True, timeout=10, check=True)
        actual = json.loads(result.stdout)['data']
        self.assertEqual(actual['taskId'], child['taskId'])
        deadline = time.monotonic() + 5
        while not worker_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(worker_pid.exists())
        original_invoke = MOD.invoke
        calls = []
        def local_cli(runtime, args, seconds, guard=lambda: None, record=None, failed_reply=False):
            calls.append(list(map(str, args)))
            if args[:3] == ['http', 'comparison', 'stop']:
                return {'stopRequested': True, 'errors': []}  # Explicit fixture: no Docker anchors exist.
            return original_invoke(runtime, args, seconds, guard, record, failed_reply)
        controller.stop_child = MOD.Controller.stop_child.__get__(controller)
        controller.cli = MOD.Controller.cli.__get__(controller)
        with patch.object(MOD, 'invoke', side_effect=local_cli):
            cleanup = controller.stop_child(child)
        self.assertTrue(cleanup['taskTerminalProven'])
        self.assertFalse(cleanup['cleanupProven'])
        state = subprocess.run(argv + ['--root', controller.plan['taskRoot'], 'task', 'status', '--id', child['taskId']],
                               capture_output=True, text=True, timeout=10, check=True)
        self.assertEqual(json.loads(state.stdout)['data']['state'], 'cancelled')
        self.assertEqual(len([args for args in calls if 'cancel' in args]), 1)
        self.assertFalse(any('run' in args for args in calls))
        pid = int(worker_pid.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_real_controller_term_loses_foreground_response_then_cancels_only_original_task(self):
        controller = self.controller()
        child = controller.prepare_point(self.points[0])
        folder = Path(child['directory'])
        worker_pid = self.root / 'term-worker-pid'
        script = folder / 'local-worker.py'
        script.write_text('import os,time,pathlib\npathlib.Path(' + repr(str(worker_pid)) +
                          ').write_text(str(os.getpid()))\ntime.sleep(60)\n')
        task_source = folder / 'local-task-source.json'
        MOD.save(task_source, {'schemaVersion': 1, 'kind': 'component-task', 'name': 'capacity-term-fixture',
            'command': [str(Path(sys.executable).resolve()), '-B', str(script)], 'cwd': str(folder),
            'trackedFiles': [str(script)], 'effect': 'filesystem', 'timeoutMs': 65000,
            'terminateGraceMs': 500, 'maxLogBytes': 65536})
        argv = [str(self.node), str(ROOT / 'dist/cli.js')]
        subprocess.run(argv + ['task', 'plan', str(task_source), '--out', child['taskPlan']],
                       capture_output=True, text=True, timeout=10, check=True)
        task = MOD.read_json(child['taskPlan'])
        child.update(taskPlanDigest=task['digest'], taskPlanSha256=MOD.entry(child['taskPlan'])['sha256'],
                     taskTimeoutMs=65000)
        code = '''
import importlib.util,json,signal
spec=importlib.util.spec_from_file_location('capacity_term_fixture',COMPONENT)
mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
mod.os.geteuid=lambda:1000
mod.platform.system=lambda:'Linux'
signal.signal(signal.SIGTERM,mod.cancelled)
c=mod.Controller(ROOT)
c.source_current=lambda:None
c.clean=lambda name:None
c.prepare_point=lambda point:CHILD
original=c.cli
def cli(args,seconds=10,record=None,failed_reply=False,long=False):
    if args[:3]==['http','capacity','verify']:
        return {'valid':True,'planDigest':'c'*64,'contentDigest':'d'*64,'plan':CAPACITY}
    if args[:3]==['http','comparison','stop']:
        return {'stopRequested':True,'errors':[]}
    return original(args,seconds,record,failed_reply,long)
c.cli=cli
print(json.dumps(c.run()))
'''.replace('COMPONENT', repr(str(COMPONENT))).replace('ROOT', repr(str(self.root))).replace(
            'CHILD', repr(child)).replace('CAPACITY', repr(self.capacity))
        process = subprocess.Popen([sys.executable, '-B', '-c', code], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            while not worker_pid.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(worker_pid.exists())
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            result = json.loads(stdout)
            self.assertFalse(result['ok'])
            self.assertEqual(result['points'][0]['state'], 'submission-response-unknown')
            self.assertEqual(result['points'][1]['state'], 'not-started')
            self.assertTrue(result['cleanup']['taskTerminalProven'])
            status = subprocess.run(argv + ['--root', controller.plan['taskRoot'], 'task', 'status',
                '--id', child['taskId']], capture_output=True, text=True, timeout=5, check=True)
            self.assertEqual(json.loads(status.stdout)['data']['state'], 'cancelled')
            with self.assertRaises(ProcessLookupError):
                os.kill(int(worker_pid.read_text()), 0)
            self.assertEqual(len(list((self.root / 'task-state/tasks').iterdir())), 1)
        finally:
            subprocess.run(argv + ['--root', controller.plan['taskRoot'], 'task', 'cancel', '--id', child['taskId']],
                           capture_output=True, timeout=5, check=False)
            if process.poll() is None:
                process.kill()
            process.communicate()


if __name__ == '__main__':
    unittest.main()
