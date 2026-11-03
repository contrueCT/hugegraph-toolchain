# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Total-deadline protocol fixtures; no Docker/HTTP/SSH or performance claims."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

FILE = Path(__file__).with_name('test_http_comparison_workflow.py')
SPEC = importlib.util.spec_from_file_location('deadline_existing_protocol_fixture', FILE)
FIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIX)
M = FIX.MOD


class TotalDeadline(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hgbench-total-deadline-')
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        M.CANCELLED = False
        self.addCleanup(setattr, M, 'CANCELLED', False)

    def controller(self):
        op = object.__new__(M.Controller)
        op.started = 100
        op.plan = {'budgets': {'budgetMode': 'total', 'maxTotalMilliseconds': 200000,
                               'stopMs': 30000, 'sealAndAnalyzeMs': 40000}}
        op.root, op.cleanup, op.cleanup_deadline, op.seal_deadline = self.root, False, None, None
        op.arm_deadline = None
        op.settings = {'supervisor': {'commandMs': 100000, 'maxCommandBytes': 65536}}
        op.env = dict(os.environ)
        op.last_disk = 100000
        return op

    def test_work_cleanup_and_seal_share_one_absolute_total_end(self):
        op = self.controller()
        self.assertEqual(op.total_end(), 300)
        self.assertEqual(op.active_end(), 260)
        with patch.object(M.time, 'monotonic', return_value=259):
            op.running_deadline()
            self.assertEqual(op.phase_end(40000, 1000), 299)
        with patch.object(M.time, 'monotonic', return_value=260):
            with self.assertRaisesRegex(M.WorkflowError, 'total-active-deadline'):
                op.running_deadline()
        op.cleanup, op.cleanup_deadline = True, 290
        with patch.object(M.time, 'monotonic', return_value=289):
            op.guard()
        with patch.object(M.time, 'monotonic', return_value=291):
            with self.assertRaisesRegex(M.WorkflowError, 'total-cleanup-or-seal-deadline'):
                op.guard()
        op.cleanup_deadline, op.seal_deadline = None, 299
        with patch.object(M.time, 'monotonic', return_value=299):
            with self.assertRaises(M.WorkflowError):
                op.guard()

    def test_command_reserves_term_reap_inside_total_and_legacy_has_no_new_arguments(self):
        op = self.controller()
        with patch.object(M.time, 'monotonic', return_value=250), patch.object(M, 'command', return_value={}) as run:
            op.cmd(['fixed-command'], 100000)
        self.assertEqual(run.call_args.args[1], 7)
        self.assertEqual(run.call_args.kwargs, {'terminate_grace': 3, 'terminate_deadline': 260})
        op.cleanup, op.cleanup_deadline = True, 290
        with patch.object(M.time, 'monotonic', return_value=288), patch.object(M, 'command') as run:
            with self.assertRaisesRegex(M.WorkflowError, 'total-command-reserve-exhausted'):
                op.cmd(['fixed-command'])
            run.assert_not_called()
        del op.plan['budgets']['budgetMode']
        op.cleanup = False
        with patch.object(M, 'command', return_value={}) as run:
            op.cmd(['fixed-command'])
        self.assertEqual(run.call_args.kwargs, {})
        self.assertEqual(run.call_args.args[1], 100)

    def run_fixture(self, preflight_failure=None, budget_expired=False, cancel_during_bridge=False):
        op = FIX.RunFixture(self.root)
        op.plan['budgets']['budgetMode'] = 'total'
        op.seal_deadline = None
        op.preflight_failure = preflight_failure
        if cancel_during_bridge:
            original = op.cmd
            def command(argv, *args, **kwargs):
                if any(str(arg).endswith('/paired-evidence.mjs') for arg in argv):
                    M.CANCELLED = True
                return original(argv, *args, **kwargs)
            op.cmd = command
        # Mock observation task order and resources from the existing protocol fixture.
        # This does not execute or validate real Docker resources.
        if budget_expired:
            op.started -= 1000
            op.guard = lambda: M.Controller.guard(op)
        def verify(*_args, **kwargs):
            if kwargs.get('check'):
                kwargs['check']()
        with patch.object(M.platform, 'system', return_value='Linux'), \
             patch.object(M.platform, 'release', return_value='fixture'), \
             patch.object(M.platform, 'machine', return_value='fixture'), \
             patch.object(M.os, 'getuid', return_value=1000), \
             patch.object(M.os, 'getgid', return_value=1000), \
             patch.object(M, 'proc_read', return_value='host-boot-fixture'), \
             patch.object(M, 'load_attempt', side_effect=verify):
            result = op.run()
        return op, result

    def test_successful_total_keeps_registration_before_any_arm_and_flags_unqualified(self):
        op, result = self.run_fixture()
        self.assertTrue(result['ok'])
        self.assertTrue(op.assert_registration)
        self.assertEqual(op.order, ['arm-01', 'arm-02'])
        self.assertEqual(result['plannedArms'], 2)
        self.assertEqual(result['budgetMode'], 'total')
        self.assertFalse(result['partial'])
        self.assertFalse(result['performanceAccepted'])
        self.assertFalse(result['statisticsEligible'])
        lock = M.read_json(self.root / 'execution/execution.lock.json')
        self.assertIn('terminal.json', [row['path'] for row in lock['files']])

    def test_preflight_failure_retains_all_planned_receipts_with_no_dispatch(self):
        op, result = self.run_fixture(preflight_failure='runtime-source-changed')
        self.assertFalse(result['ok'])
        self.assertTrue(result['partial'])
        self.assertFalse(result['executionCompleted'])
        self.assertEqual(result['failure'], 'runtime-source-changed')
        self.assertEqual(op.order, [])
        registration = M.read_json(self.root / 'execution/execution.json')
        self.assertEqual([row['runId'] for row in registration['receipts']], ['arm-01', 'arm-02'])
        self.assertTrue((self.root / 'execution/terminal.json').is_file())

    def test_expired_total_still_retains_small_partial_terminal_and_never_runs_analysis(self):
        op, result = self.run_fixture(budget_expired=True)
        self.assertFalse(result['ok'])
        self.assertTrue(result['partial'])
        self.assertFalse(result['executionSealed'])
        self.assertEqual(result['bridgeExitCode'], None)
        self.assertEqual(op.order, [])
        self.assertEqual(result['failure'], 'total-active-deadline')
        self.assertTrue((self.root / 'result.json').is_file())
        self.assertEqual(len(M.read_json(self.root / 'execution/execution.json')['receipts']), 2)

    def test_late_cancel_during_bridge_does_not_report_uncancelled_success(self):
        op, result = self.run_fixture(cancel_during_bridge=True)
        self.assertEqual(op.order, ['arm-01', 'arm-02'])
        self.assertFalse(result['ok'])
        self.assertTrue(result['partial'])
        self.assertEqual(result['failure'], 'controller-cancelled')
        self.assertFalse(result['executionCompleted'])

    def test_exhausted_driver_cleanup_still_attempts_target_without_resetting_total_deadline(self):
        op = FIX.FakeController(self.root, 'pre')
        op.started = 100.0
        op.plan['budgets'].update(budgetMode='total', maxTotalMilliseconds=100000)
        op.seal_deadline = op.cleanup_deadline = None
        op.last_disk = 100.0
        op.env = {}
        op.guard = lambda: M.Controller.guard(op)
        clock = [100.0]
        original_probe = op.mode_probe
        def probe(*args):
            clock[0] = 159.0  # Original active cutoff=160, hard end=200.
            return original_probe(*args)
        op.mode_probe = probe
        deadlines = []
        def driver(*_):
            op.events.append('driver-stop')
            deadlines.append(op.cleanup_deadline)
            clock[0] = 190.0  # Exhaust shared stop window; no new budget for target.
            raise M.WorkflowError('synthetic-driver-stop-timeout')
        def target(*_):
            op.events.append('target-stop')
            deadlines.append(op.cleanup_deadline)
            op.cmd(['must-not-spawn-after-deadline'])
        op.stop_driver, op.stop_target = driver, target
        arm = {'runId': 'arm-' + '1' * 40, 'blockId': 'b01', 'arm': 'A', 'position': 0,
               'expectedMode': 'forward', 'maxMilliseconds': 300000, 'maxEvidenceBytes': 32 * M.MiB}
        with patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]),              patch.object(M, 'process_identity', side_effect=lambda role, pid: {'role': role, 'hostPid': pid}),              patch.object(M, 'command') as command:
            with self.assertRaisesRegex(M.WorkflowError, 'arm-failed-see-preserved-receipt'):
                op.arm(arm)
            command.assert_not_called()
        self.assertEqual(deadlines, [189.0, 189.0])
        self.assertLess(op.events.index('driver-stop'), op.events.index('target-stop'))
        receipt = M.read_json(self.root / 'execution/arms' / arm['runId'] / 'receipt.json')
        self.assertTrue(receipt['partial'])
        self.assertIn('synthetic-driver-stop-timeout', receipt['cleanupErrors'])
        self.assertIn('total-command-reserve-exhausted', receipt['cleanupErrors'])
        self.assertEqual(receipt['totalDeadlineMonotonicNs'], '200000000000')
        self.assertIsNone(op.cleanup_deadline)

    def test_real_timeout_uses_term_to_allow_nested_owner_cleanup_before_group_kill(self):
        # Actual local surrogate parser owner; its child has an independent process group.
        # This verifies cancellation protocol, not JVM resource or phase provenance.
        receipt, marker = self.root / 'command.json', self.root / 'cleaned.json'
        code = ('import json,os,signal,subprocess,sys,time\n'
                'child=subprocess.Popen([sys.executable,"-B","-c","import time; time.sleep(60)"],start_new_session=True)\n'
                'def stop(s,f):\n'
                ' os.killpg(child.pid,signal.SIGKILL); child.wait(); open(sys.argv[1],"w").write(json.dumps({"child":child.pid})); sys.exit(143)\n'
                'signal.signal(signal.SIGTERM,stop)\n'
                'print(os.getpid(),flush=True)\n'
                'time.sleep(60)\n')
        begun = time.monotonic()
        with self.assertRaisesRegex(M.WorkflowError, 'command-deadline-exceeded'):
            M.command([sys.executable, '-B', '-c', code, str(marker)], .3, 65536,
                      dict(os.environ), record=receipt, terminate_grace=3, terminate_deadline=begun + 5)
        self.assertLess(time.monotonic() - begun, 4)
        value = M.read_json(receipt)
        child = M.read_json(marker)['child']
        self.assertEqual(value['error'], 'command-deadline-exceeded')
        with self.assertRaises(ProcessLookupError):
            os.kill(child, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(value['stdout']), 0)


if __name__ == '__main__':
    unittest.main()
