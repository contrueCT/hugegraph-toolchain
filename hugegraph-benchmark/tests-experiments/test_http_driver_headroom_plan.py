# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Finite local planner fixtures. No Docker, HTTP requests, remote work or performance claims."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / '.hgbench/http-driver-headroom-plan.py'
SPEC = importlib.util.spec_from_file_location('driver_headroom_plan_test', COMPONENT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class HeadroomPlan(unittest.TestCase):
    def test_fixed_schedule_and_sample_policy_do_not_claim_capacity_or_retroactive_aa(self):
        rows = MOD.schedule()
        self.assertEqual(len(rows), 12)
        self.assertEqual(len({r['cellId'] for r in rows}), 12)
        for block in ('b01', 'b02'):
            for rate in MOD.RATES:
                pair = [row for row in rows if row['blockId'] == block and row['ratePerSecond'] == rate]
                self.assertEqual([r['driverCpus'] for r in pair], [1, 2] if block == 'b01' else [2, 1])
        for rate in MOD.RATES:
            settings = MOD.suite_settings('test', rate, [])
            self.assertEqual(settings['caseIds'], ['scalar-add', 'rows-4096', 'string-1024k'])
            self.assertEqual(settings['loads'][0]['maxInFlight'], 8)
            self.assertEqual(settings['durationMs'] * rate // 1000, rate * 120)
            self.assertLess(MOD.WINDOW_POLICY['windowMs'] * rate // 1000, MOD.WINDOW_POLICY['minimumP99Requests'])
            self.assertNotIn('clockReference', settings)
        contract = MOD.analysis_contract()
        self.assertFalse(contract['performanceAccepted'])
        self.assertIsNone(contract['warmupStable'])
        self.assertIsNone(contract['driverHeadroomAccepted'])
        self.assertIsNone(contract['serverCapacityUpperBound'])
        self.assertTrue(any('AA004' in value for value in contract['boundaries']))
        self.assertEqual(contract['driverPressureSignals']['analyzerAbsoluteProcessCpuCores'], 0.8)
        self.assertIn('never replaces or clears', contract['driverPressureSignals']['originalAnalyzerGate'])

    def test_budgets_cover_driver_target_arm_parent_and_whole_phase_counts(self):
        b = MOD.budgets()
        self.assertEqual(b['workflowDeadlineMs'], 549000)
        self.assertEqual(b['httpTaskTimeoutMs'], 609000)
        self.assertEqual(b['httpWaitMs'], 644000)
        self.assertEqual(b['driverLifetimeRequiredMs'], 730000)
        self.assertEqual(b['targetLifetimeRequiredMs'], 1165000)
        self.assertLessEqual(b['driverLifetimeRequiredMs'], b['driverTtlMs'])
        self.assertLessEqual(b['targetLifetimeRequiredMs'], b['targetTtlMs'])
        self.assertEqual(b['armMaximumMs'], sum(b['stepMaximumMs'].values()))
        self.assertLessEqual(12 * b['armMaximumMs'] + b['sealReserveMs'], b['operationMaximumMs'])
        self.assertGreaterEqual(b['parentRuntimeMaxSeconds'] * 1000, b['operationMaximumMs'] + b['operationCleanupMs'])
        self.assertEqual(b['actualNodeSessions'], 108)
        self.assertEqual(b['plannedTraceRecordsPerArm']['40'], 18006)
        self.assertGreaterEqual(b['prepareRequiredFreeBytes'], b['maxWorkBytes'] + b['minFreeBytes'])
        self.assertEqual(b['reservedRetainedBytesIncludingSeal'], 12 * (512 + 512 + 96 + 64) * MOD.MiB + 256 * MOD.MiB)
        self.assertLessEqual(b['reservedRetainedBytesIncludingSeal'], b['maxWorkBytes'])

    def test_gc_unit_absence_is_explicit_current_inactivity_not_prior_success(self):
        unit = 'old.service'
        fields = {'Id': unit, 'LoadState': 'not-found', 'ActiveState': 'inactive', 'SubState': 'dead',
                  'MainPID': '0', 'ControlGroup': '', 'Result': 'success'}
        raw = '\n'.join(key + '=' + value for key, value in fields.items()) + '\n'
        observation = MOD.job_snapshot(unit, raw)
        self.assertEqual(observation['stateEvidence'], 'unit-absent')
        self.assertEqual(observation['raw'], raw)
        self.assertFalse(observation['priorTaskSuccessProven'])
        for key, value in (('Id', 'other.service'), ('LoadState', 'error'), ('ActiveState', 'active'),
                           ('SubState', 'running'), ('MainPID', '12'), ('ControlGroup', '/live')):
            changed = dict(fields, **{key: value})
            with self.assertRaises(ValueError):
                MOD.job_snapshot(unit, '\n'.join(k + '=' + v for k, v in changed.items()))
        for missing in ('Id', 'Result', 'ControlGroup'):
            changed = {key: value for key, value in fields.items() if key != missing}
            with self.assertRaises(ValueError):
                MOD.job_snapshot(unit, '\n'.join(k + '=' + v for k, v in changed.items()))
        with self.assertRaises(ValueError):
            MOD.job_snapshot(unit, raw + 'Id=' + unit + '\n')
        active = dict(fields, LoadState='loaded', ActiveState='active', SubState='running', MainPID='7', ControlGroup='/d6')
        self.assertEqual(MOD.job_snapshot(unit, '\n'.join(k + '=' + v for k, v in active.items()), True)['stateEvidence'], 'observed-only')

    def test_recipe_uses_real_public_commands_fixed_cpu_delta_owned_cids_and_no_dispatch(self):
        runtime = MOD.H.read(MOD.H.TEMPLATES / 'runtime-settings.json')
        first, second = MOD.schedule()[:2]
        for row in (first, second):
            plan = MOD.sequence_arm(Path('/operation'), Path('/fixed-source'), runtime, row, 'a' * 16, False, '1000:1000')
            operations = {item['id']: item for item in plan['operations']}
            create = operations['create-owned-driver']['argvTemplate']
            self.assertEqual(create[create.index('--cpus') + 1], str(row['driverCpus']))
            self.assertEqual(create[create.index('--network') + 1], 'container:{{target.container.id}}')
            self.assertEqual(create[-1], 'import time; time.sleep(750)')
            self.assertEqual(operations['plan-http-inside-driver']['maximumMs'], 30000)
            self.assertNotIn('task', operations['plan-http-inside-driver']['argvTemplate'])
            submit = operations['run-http']['argvTemplate']
            self.assertNotIn('--detach', submit)
            self.assertEqual(operations['run-http']['maximumMs'], 644000)
            self.assertIn('status', operations['run-http']['recoveryOnlyArgvTemplate'])
            self.assertEqual(submit[submit.index('--request-id') + 1], plan['requestId'])
            order = [item['id'] for item in plan['operations']]
            self.assertLess(order.index('stop-remove-owned-driver'), order.index('mode-post'))
            self.assertLess(order.index('remove-owned-target'), order.index('rebuild-phase-windows'))
            self.assertTrue(plan['failureCleanup']['noFreshDispatch'])
            self.assertTrue(any('query failure must not skip' in action for action in plan['failureCleanup']['actions']))
            self.assertEqual(operations['rebuild-phase-windows']['argvTemplate'][-1], '1000')

    def test_actual_local_public_cli_generates_all_12_plans_and_preserves_new_directory_contract(self):
        # This is the actual offline v6 CLI, not a mocked planner. No task run occurs.
        with tempfile.TemporaryDirectory(prefix='hgbench-headroom-plan-') as folder:
            out = Path(folder) / 'draft'
            argv = [sys.executable, '-B', str(COMPONENT), 'draft', '--out', str(out), '--id', 'headroom-fixture']
            result = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stderr)
            reply = json.loads(result.stdout)
            self.assertFalse(reply['executed'])
            self.assertFalse(reply['dispatchImplemented'])
            self.assertEqual(reply['publicHttpPlannerCalls'], 36)
            sequence = MOD.H.read(out / 'sequence.json')
            self.assertFalse(sequence['hostHttpPlansExecutable'])
            self.assertTrue(sequence['runtimeBindingsPending'])
            self.assertEqual(len(sequence['physicalArms']), 12)
            self.assertEqual(len(MOD.H.read(out / 'assessment-template.json')['cells']), 36)
            for row in MOD.schedule():
                arm = out / 'arms' / row['cellId']
                profile = MOD.H.read(arm / 'profile.json')
                self.assertEqual(profile['benchmark']['coverage'], 'subset')
                self.assertEqual(profile['benchmark']['purpose'], 'calibration')
                self.assertEqual(profile['loads'][0]['ratePerSecond'], row['ratePerSecond'])
                self.assertEqual(MOD.H.read(arm / 'http-plan-host/task.source.json')['timeoutMs'], 609000)
                self.assertFalse((arm / 'environment').exists())
                self.assertTrue(all(path.startswith('/evidence/') for path in MOD.H.read(arm / 'driver-evidence/profile.json')['target']['evidenceFiles']))
            lock_raw = (out / 'plan.lock.json').read_bytes()
            lock = json.loads(lock_raw)
            self.assertEqual(MOD.H.value_sha(lock['files']), lock['contentDigest'])
            for member in lock['files']:
                self.assertEqual(MOD.H.digest_file(out / member['path']), {key: member[key] for key in ('bytes', 'sha256')})
            second = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual((out / 'plan.lock.json').read_bytes(), lock_raw)

    def test_bad_id_and_protected_unit_rejected_without_output(self):
        with tempfile.TemporaryDirectory(prefix='hgbench-headroom-invalid-') as folder:
            out = Path(folder) / 'new'
            for extra in (['--id', '../bad'], ['--require-inactive-unit', MOD.H.PROTECTED_UNIT]):
                result = subprocess.run([sys.executable, '-B', str(COMPONENT), 'draft', '--out', str(out), *extra],
                                        cwd=ROOT, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(out.exists())


if __name__ == '__main__':
    unittest.main()
