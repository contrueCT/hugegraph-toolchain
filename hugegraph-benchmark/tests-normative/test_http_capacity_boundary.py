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
"""Explicit controller/Docker CLI protocol fixtures; no real Docker or target runtime proof."""
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import test_http_capacity_workflow as legacy
MOD = legacy.MOD


class Boundary(unittest.TestCase):
    def fixture(self):
        base = legacy.CapacityWorkflow('test_runtime_forbids_overrides_unknown_fields_and_unbounded_cleanup')
        base.setUp()
        self.addCleanup(base.doCleanups)
        return base

    def controller(self, action='stop-case', cleanup=True):
        base = self.fixture()
        base.points[1]['caseId'] = base.points[0]['caseId']
        base.points[1]['ratePerSecond'] = 20
        base.points[1]['mode'] = 'embedded'
        base.capacity['policy']['continuation'] = {'schemaVersion': 1, 'mode': 'verified-case-range',
            'maxVerificationMilliseconds': 360000, 'maxVerificationBytes': 2 * MOD.MiB}
        MOD.save(base.root / 'capacity/capacity-plan.json', base.capacity, replace=True)
        workflow = MOD.read_json(base.root / 'workflow.json')
        workflow['points'] = base.points
        MOD.save(base.root / 'workflow.json', workflow, replace=True)
        MOD.save(base.root / 'workflow.lock.json', {'schemaVersion': 1, 'kind': 'http-capacity-workflow-lock',
            'attemptId': '1' * 32, 'planSha256': MOD.entry(base.root / 'workflow.json')['sha256']}, replace=True)
        controller = base.controller()
        prepare, cli = controller.prepare_point, controller.cli
        def prepared(point):
            child = prepare(point)
            path = Path(child['directory']) / 'result.json'
            value = MOD.read_json(path)
            value.update(failure=None, bridgeError=None, bridgeExitCode=0)
            if point['id'] == base.points[0]['id']:
                value.update(ok=False, evidenceValid=False)
            MOD.save(path, value, replace=True)
            return child
        def invoke(args, *rest, **kwargs):
            value = cli(args, *rest, **kwargs)
            if 'run' in args and base.points[0]['requestId'] in args:
                value[0]['state'] = 'failed'
                return value[0], 6
            return value
        controller.prepare_point = prepared
        controller.cli = invoke
        controller.daemon = lambda *args: {'ID': 'protocol-fixture'}
        def boundary(point, child, policy):
            return {'action': action if point['id'] == base.points[0]['id'] else 'continue',
                    'issues': [], 'symptoms': ['driver-range-observed'], 'rawQualityValid': False}
        controller.boundary = boundary
        def owned(*args):
            if not cleanup:
                raise MOD.COMMON.WorkflowError('capacity-owned-absence-unproven')
        controller.owned_absent = owned
        return base, controller

    def test_verified_case_stop_keeps_failed_task_and_skipped_id_and_continues_other_ten_questions(self):
        base, controller = self.controller()
        result = base.run_controller(controller)
        self.assertTrue(result['ok'])
        self.assertEqual(len(result['points']), 11)
        trigger, skipped, next_case = result['points'][:3]
        self.assertEqual(trigger['state'], 'failed')
        self.assertTrue(trigger['caseStopTrigger'])
        self.assertFalse(trigger['boundaryDecision']['rawQualityValid'])
        self.assertEqual(skipped['state'], 'not-started')
        self.assertEqual(skipped['triggerPointId'], trigger['id'])
        self.assertEqual(skipped['taskId'], base.points[1]['taskId'])
        self.assertFalse(skipped['dispatchRecorded'])
        self.assertEqual(next_case['state'], 'completed')
        self.assertEqual(sum('run' in c for c in base.calls), 10)
        self.assertEqual(len(MOD.read_json(base.root / 'execution-set.json')['points']), 10)
        lock = MOD.read_json(base.root / 'evidence.lock.json')
        self.assertIn('observations/docker-daemon-baseline.json', [row['path'] for row in lock['files']])

    def test_unexplained_projection_or_failed_owned_cleanup_stops_everything(self):
        for action, cleanup in [('stop-study', True), ('stop-case', False)]:
            with self.subTest(action=action, cleanup=cleanup):
                base, controller = self.controller(action, cleanup)
                result = base.run_controller(controller)
                self.assertFalse(result['ok'])
                self.assertEqual(result['points'][0]['state'], 'failed')
                self.assertTrue(all(row['state'] == 'not-started' for row in result['points'][1:]))
                self.assertEqual(sum('run' in c for c in base.calls), 1)

    def test_failed_task_with_successful_component_result_is_unexplained_and_never_advances(self):
        base, controller = self.controller()
        prepare = controller.prepare_point
        def successful(point):
            child = prepare(point)
            path = Path(child['directory']) / 'result.json'
            value = MOD.read_json(path)
            value.update(ok=True, evidenceValid=True)
            MOD.save(path, value, replace=True)
            return child
        controller.prepare_point = successful
        result = base.run_controller(controller)
        self.assertFalse(result['ok'])
        self.assertEqual(result['points'][0]['state'], 'failed')
        self.assertEqual(sum('run' in c for c in base.calls), 1)

    def test_fixed_boundary_uses_cooperative_supervisor_and_existing_absolute_point_deadline(self):
        base = self.fixture()
        controller = base.controller()
        child = controller.prepare_point(base.points[0])
        original = time.monotonic() + 2
        controller.step_deadline = original
        calls = []
        def supervised(argv, seconds, cap, env, guard, record):
            calls.append((argv, seconds, cap, controller.step_deadline))
            self.assertLessEqual(seconds, 2)
            self.assertEqual(controller.step_deadline, original)
            self.assertEqual(argv[2], '__boundary')
            MOD.save(argv[argv.index('--out') + 1], {
                'kind': 'http-capacity-boundary-projection', 'schemaVersion': 1,
                'capacityPlanDigest': controller.plan['capacityPlanDigest'],
                'capacityContentDigest': controller.plan['capacityContentDigest'],
                'pointId': base.points[0]['id'], 'comparisonPlanDigest': base.points[0]['comparisonPlanDigest'],
                'executionLockSha256': MOD.entry(Path(child['directory']) / 'execution/execution.lock.json')['sha256'],
                'plannedArms': 4, 'observedArms': 4, 'action': 'continue', 'performanceAccepted': False})
            raw = MOD.COMMON.read_bytes(argv[argv.index('--out') + 1], MOD.MiB)
            return {'exitCode': 0, 'stdout': json.dumps({'ok': True, 'receiptSha256': MOD.COMMON.sha(raw)})}
        with patch.object(MOD, 'confirmation_command', side_effect=supervised):
            self.assertEqual(controller.boundary(base.points[0], child,
                {'maxVerificationMilliseconds': 360000, 'maxVerificationBytes': 2 * MOD.MiB})['action'], 'continue')
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], 65536)
        self.assertEqual(controller.step_deadline, original)
        output = base.root / 'observations' / (base.points[0]['id'] + '-boundary.json')
        output.unlink()
        def changed(*args):
            result = supervised(*args)
            output.write_bytes(output.read_bytes() + b' ')
            return result
        with patch.object(MOD, 'confirmation_command', side_effect=changed):
            with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'capacity-boundary-receipt-changed'):
                controller.boundary(base.points[0], child,
                    {'maxVerificationMilliseconds': 360000, 'maxVerificationBytes': 2 * MOD.MiB})
        self.assertEqual(controller.step_deadline, original)

    def docker_fixture(self, mode='absent', single=False):
        base = self.fixture()
        if single:
            base.single_fixture()
        controller = base.controller()
        # Protocol-process startup has its own explicit test budget, outside any HTTP measurement.
        # The production check still shares one unchanged 30-second absolute endpoint deadline.
        controller.runtime['budgets']['commandMilliseconds'] = 3000
        socket_directory = tempfile.TemporaryDirectory()
        self.addCleanup(socket_directory.cleanup)
        endpoint = socket.socket(socket.AF_UNIX)
        path = Path(socket_directory.name).resolve() / 'docker.sock'
        endpoint.bind(str(path))
        self.addCleanup(endpoint.close)
        target_image = 'sha256:' + 'b' * 64
        MOD.save(base.root / 'capacity/project/settings.json', {'dockerSocket': str(path), 'image': target_image})
        MOD.save(base.root / 'capacity/project/environment.json', {'docker': {'serverVersion': 'fixture'}})
        executable = base.root / 'docker'
        executable.write_text('#!' + sys.executable + '\n' +
            'import json,sys\n' +
            'args=sys.argv[1:]\n' +
            ('print(json.dumps({"ID":"different-daemon","ServerVersion":"fixture"}))\n' if mode == 'wrong-daemon' else
             'if "info" in args: print(json.dumps({"ID":"fixture-daemon","ServerVersion":"fixture"}))\n'
             'else:\n' +
             (' print("[{\\"Id\\":\\"remaining\\"}]")\n' if mode == 'remaining' else
              ' print("[]"); print("Error: No such container: "+' +
              ('"wrong-id"' if mode == 'wrong-id' else 'args[-1]') + ',file=sys.stderr); sys.exit(1)\n')))
        executable.chmod(0o700)
        controller.runtime['comparisonRuntime']['dockerExecutable'] = str(executable)
        controller.plan['identities']['docker'] = MOD.entry(executable)
        baseline = {'ID': 'fixture-daemon', 'ServerVersion': 'fixture', 'socketPath': str(path),
                    'device': path.stat().st_dev, 'inode': path.stat().st_ino}
        run_ids = [arm['runId'] for arm in controller.point_schedule(base.points[0])]
        owned = [{'runId': run_ids[i // 2], 'role': 'target' if i % 2 else 'driver',
                  'id': format(i + 1, '064x'), 'imageId': target_image if i % 2 else controller.runtime['comparisonRuntime']['driver']['image']}
                 for i in range(2 * len(run_ids))]
        return base, controller, baseline, {'ownedContainers': owned}

    def test_single_observation_checks_exact_original_two_cids_and_rejects_foreign_run(self):
        base, controller, baseline, decision = self.docker_fixture(single=True)
        self.assertEqual(len(controller.point_schedule(base.points[0])), 1)
        controller.owned_absent(base.points[0], decision, baseline)
        proof = MOD.read_json(base.root / 'observations' / (base.points[0]['id'] + '-owned-proof.json'))
        self.assertEqual(len(proof['containers']), 2)
        self.assertTrue(proof['cleanupProven'])
        base, controller, baseline, decision = self.docker_fixture(single=True)
        decision['ownedContainers'][0]['runId'] = controller.point_schedule(base.points[1])[0]['runId']
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'owned-container-inventory'):
            controller.owned_absent(base.points[0], decision, baseline)
        self.assertFalse((base.root / 'observations' / (base.points[0]['id'] + '-owned-proof.json')).exists())

    def test_single_boundary_requires_one_real_observation_and_rejects_legacy_four_arm_receipt(self):
        base = self.fixture()
        base.single_fixture()
        controller = base.controller()
        child = controller.prepare_point(base.points[0])
        count = 1
        def supervised(argv, seconds, cap, env, guard, record):
            output = Path(argv[argv.index('--out') + 1])
            MOD.save(output, {'kind': 'http-capacity-boundary-projection', 'schemaVersion': 1,
                'capacityPlanDigest': controller.plan['capacityPlanDigest'],
                'capacityContentDigest': controller.plan['capacityContentDigest'],
                'pointId': base.points[0]['id'], 'comparisonPlanDigest': base.points[0]['comparisonPlanDigest'],
                'executionLockSha256': MOD.entry(Path(child['directory']) / 'execution/execution.lock.json')['sha256'],
                'plannedArms': count, 'observedArms': count, 'action': 'continue', 'performanceAccepted': False})
            raw = MOD.COMMON.read_bytes(output, MOD.MiB)
            return {'exitCode': 0, 'stdout': json.dumps({'ok': True, 'receiptSha256': MOD.COMMON.sha(raw)})}
        with patch.object(MOD, 'confirmation_command', side_effect=supervised):
            self.assertEqual(controller.boundary(base.points[0], child,
                {'maxVerificationMilliseconds': 360000, 'maxVerificationBytes': 2 * MOD.MiB})['plannedArms'], 1)
        (base.root / 'observations' / (base.points[0]['id'] + '-boundary.json')).unlink()
        for count in [4, True]:
            with self.subTest(count=count):
                with patch.object(MOD, 'confirmation_command', side_effect=supervised):
                    with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'boundary-identity'):
                        controller.boundary(base.points[0], child,
                            {'maxVerificationMilliseconds': 360000, 'maxVerificationBytes': 2 * MOD.MiB})
                (base.root / 'observations' / (base.points[0]['id'] + '-boundary.json')).unlink()

    def test_actual_fixed_docker_protocol_requires_exact_absence_same_daemon_and_distinct_role_images(self):
        base, controller, baseline, decision = self.docker_fixture()
        controller.owned_absent(base.points[0], decision, baseline)
        proof = MOD.read_json(base.root / 'observations' / (base.points[0]['id'] + '-owned-proof.json'))
        self.assertTrue(proof['cleanupProven'])
        self.assertEqual(len(proof['containers']), 8)
        commands = list((base.root / 'observations').glob('*-owned-*.json'))
        self.assertGreaterEqual(len(commands), 11)

    def test_remaining_wrong_cid_unknown_daemon_or_reused_cid_never_proves_cleanup(self):
        for mode in ['remaining', 'wrong-id', 'wrong-daemon', 'duplicate', 'wrong-image']:
            with self.subTest(mode=mode):
                base, controller, baseline, decision = self.docker_fixture(mode)
                if mode == 'duplicate':
                    decision['ownedContainers'][0]['id'] = decision['ownedContainers'][1]['id']
                if mode == 'wrong-image':
                    decision['ownedContainers'][1]['imageId'] = controller.runtime['comparisonRuntime']['driver']['image']
                with self.assertRaises(MOD.COMMON.WorkflowError):
                    controller.owned_absent(base.points[0], decision, baseline)
                self.assertFalse((base.root / 'observations' / (base.points[0]['id'] + '-owned-proof.json')).exists())


if __name__ == '__main__':
    unittest.main()
