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

"""Execution/fault contracts with real child processes, no database or benchmark.

The child simulates the Java output protocol. Passing these checks does not prove
Neo4j compatibility, actual Docker controls, or SF100 correctness/performance.
"""
import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('execution_fixture', ROOT/'tests-normative/test_ldbc_reference_verify.py')
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
verify = fixture.verify
RESOURCES = {'cpus': 1, 'memoryBytes': 1024**3, 'heapMiB': 256, 'pidsMax': 128,
             'workspaceMaxBytes': 4*1024**2, 'maxLogBytes': 4096}
CONTROLS = {'cpu.max': '100000 100000', 'memory.max': str(1024**3), 'memory.swap.max': '0',
            'pids.max': '128', 'pids.current': '2', 'memory.events': {'oom': 0, 'oom_kill': 0},
            'pids.events': {'max': 0}}

CHILD = r'''
from pathlib import Path
import json,os,shutil,signal,sys,time
planfile,result,mode=Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[3]
print('process protocol simulator; no DB connections',flush=True)
if mode=='fail':raise SystemExit(7)
if mode=='missing':raise SystemExit(0)
if mode=='logs':print('x'*1000000,flush=True);time.sleep(60)
if mode=='cancel':os.kill(os.getppid(),signal.SIGTERM);time.sleep(60)
if mode=='deadline':time.sleep(60)
plan=json.loads(planfile.read_text());result.mkdir()
shutil.copyfile(planfile,result/'plan.json')
tables=[]
for i,source in enumerate(plan['tables'],1):
 row={'table':source['table'],'rows':source['rows'],'batches':1 if source['rows'] else 0,
      'csvSha256':source['sha256'],'matchedInternalIdsSha256':'c'*64,'querySha256':'d'*64}
 tables.append(row)
 (result/('table-'+str(i)+'.json')).write_text(json.dumps(row))
before={'databaseInfo':plan['databaseInfo'],'nodes':plan['expected']['nodes'],
        'relationships':plan['expected']['relationships'],'indexes':[],'constraints':[],'settings':{}}
(result/'before.json').write_text(json.dumps(before))
if mode=='identity':before['databaseInfo']={**before['databaseInfo'],'id':'replacement'}
(result/'after.json').write_text(json.dumps(before))
value={'schemaVersion':1,'kind':'reference-exact-verification','allRowsCompared':True,
       'nodesMatched':plan['expected']['nodes'],'relationshipsMatched':plan['expected']['relationships'],
       'tables':tables,'referenceAnswersGenerated':False,'performanceAccepted':False}
if mode=='counts':value['nodesMatched']+=1
if mode=='tables':value['tables']=tables[:-1]
if mode=='hash':value['tables'][0]['csvSha256']='0'*64
if mode=='bool':
 for row in value['tables']:
  if row['rows']==0:row['rows']=False;break
(result/'result.json').write_text(json.dumps(value))
'''


class ReferenceExecutionTest(unittest.TestCase):
    def setUp(self):
        fixture.ReferenceVerificationPlanTest.setUp(self)
        runtime = self.args.runtime
        runtime.mkdir()
        self.java = self.root/'jdk'; (self.java/'bin').mkdir(parents=True)
        (self.java/'bin/java').write_text('not executed; real process simulator used')
        self.imported.update(javaHome=str(self.java), javaFiles=[], javaLinks=[])
        self.plan = verify.plan(self.args)
        self.plan_file = self.root/'verification-plan.json'
        self.plan_file.write_text(json.dumps(self.plan))
        self.resources = self.root/'resources.json'
        self.resources.write_text(json.dumps(RESOURCES))
        self.out = self.root/'execution'
        self.run_args = argparse.Namespace(plan=self.plan_file, import_plan=self.args.import_plan,
                                            resources=self.resources, out=self.out)
        self.mode = 'ok'
        self.children = []
        self.controls = copy.deepcopy(CONTROLS)
        self.fake_os = types.SimpleNamespace(name='posix', getuid=lambda: 1000, ST_RDONLY=os.ST_RDONLY,
                statvfs=lambda p: types.SimpleNamespace(f_flag=os.ST_RDONLY), pathsep=os.pathsep)
        patch.object(verify, 'os', self.fake_os).start()
        original_is_file = Path.is_file
        patch.object(Path, 'is_file', lambda p: str(p) == '/sys/fs/cgroup/cgroup.controllers' or
                     original_is_file(p)).start()
        patch.object(verify.imp, 'cgroup_controls', side_effect=lambda: self.controls).start()
        patch.object(verify.imp, 'verify_tree').start()
        supervise = verify.imp.supervise_process

        def child(command, work, max_logs, guard, **kwargs):
            self.children.append(command)
            control = command[command.index('--plan')+1]
            return supervise([sys.executable, '-c', CHILD, control, command[-1], self.mode],
                              work, max_logs, guard, **kwargs)
        patch.object(verify.imp, 'supervise_process', side_effect=child).start()

    def run_mode(self, mode):
        self.mode = mode
        return verify.execute(self.run_args)

    def test_success_requires_all_tables_and_frozen_java_plan(self):
        result = self.run_mode('ok')
        self.assertTrue(result['dataCorrectnessAccepted'])
        self.assertEqual(result['state'], 'verified')
        self.assertEqual(result['processExitCode'], 0)
        self.assertFalse(result['referenceAnswersGenerated'])
        self.assertFalse(result['performanceAccepted'])
        self.assertEqual(len(self.children), 1)
        self.assertIn('-Xmx256m', self.children[0])
        self.assertEqual(result['executionPlanSha256'], verify.imp.file_hash(self.out/'result/plan.json'))
        self.assertTrue((self.out/'evidence/resources.ndjson').is_file())
        self.assertEqual(json.loads((self.out/'receipt.json').read_text()), result)

    def test_zero_exit_does_not_accept_missing_incomplete_or_changed_results(self):
        for mode in ['missing', 'counts', 'tables', 'hash', 'identity', 'bool']:
            with self.subTest(mode=mode):
                self.run_args.out = self.root/('run-'+mode)
                result = self.run_mode(mode)
                self.assertEqual(result['processExitCode'], 0)
                self.assertEqual(result['state'], 'failed-no-replay')
                self.assertFalse(result['dataCorrectnessAccepted'])
                self.assertTrue((self.run_args.out/'evidence/stdout.log').is_file())

    def test_child_error_is_not_retried(self):
        result = self.run_mode('fail')
        self.assertEqual(result['processExitCode'], 7)
        self.assertFalse(result['dataCorrectnessAccepted'])
        self.assertEqual(len(self.children), 1)

    def assert_child_gone(self):
        pid = json.loads((self.out/'evidence/verifier-child.json').read_text())['pid']
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_log_budget_terminates_child_and_preserves_failure(self):
        result = self.run_mode('logs')
        self.assertFalse(result['dataCorrectnessAccepted'])
        self.assertIn('stdout/stderr budget', result['error'])
        self.assert_child_gone()

    def test_real_sigterm_preserves_partial_receipt_and_terminates_child(self):
        result = self.run_mode('cancel')
        self.assertFalse(result['dataCorrectnessAccepted'])
        self.assertIn('cancelled', result['error'])
        self.assert_child_gone()

    def test_whole_deadline_terminates_live_child(self):
        self.plan['budget'].update(maxSeconds=1, querySeconds=1)
        self.plan_file.write_text(json.dumps(self.plan))
        result = self.run_mode('deadline')
        self.assertFalse(result['dataCorrectnessAccepted'])
        self.assertIn('deadline', result['error'])
        self.assertLess(result['elapsedSeconds'], 4)
        self.assert_child_gone()

    def test_wrong_effective_controls_rejected_before_dispatch(self):
        self.controls['memory.swap.max'] = '1'
        with self.assertRaisesRegex(ValueError, 'cgroup limits'):
            self.run_mode('ok')
        self.assertFalse(self.children)
        self.assertFalse(self.out.exists())

    def test_writable_input_rejected_before_dispatch(self):
        self.fake_os.statvfs = lambda p: types.SimpleNamespace(f_flag=0)
        with self.assertRaisesRegex(ValueError, 'mounted read-only'):
            self.run_mode('ok')
        self.assertFalse(self.children)

    def test_expired_lease_rejected_before_dispatch(self):
        self.plan['leaseValidUntil'] = '2000-01-01T00:00:00Z'
        self.plan_file.write_text(json.dumps(self.plan))
        with self.assertRaisesRegex(ValueError, 'no longer spans'):
            self.run_mode('ok')
        self.assertFalse(self.children)

    def test_existing_attempt_is_never_overwritten(self):
        self.out.mkdir(); marker = self.out/'partial'; marker.write_text('keep')
        with self.assertRaises(FileExistsError):
            self.run_mode('ok')
        self.assertEqual(marker.read_text(), 'keep')
        self.assertFalse(self.children)

    def test_resource_budget_reserves_memory_and_disk_for_full_bitmaps(self):
        for values in [{**RESOURCES, 'heapMiB': 768}, {**RESOURCES, 'workspaceMaxBytes': 1024**2},
                       {**RESOURCES, 'cpus': True}]:
            with self.assertRaises(ValueError):
                verify.execution_resources(values, 256*1024**2)


if __name__ == '__main__':
    unittest.main()
