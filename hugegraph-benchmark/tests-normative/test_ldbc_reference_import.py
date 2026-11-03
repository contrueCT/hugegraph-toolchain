#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#/

"""Hand-authored contract inputs and process fault injection; never a small-graph benchmark."""
import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


imp = module('reference_import', ROOT/'tools/ldbc-reference-import.py')
fixtures = module('reference_fixture', ROOT/'tests-normative/test_ldbc_reference_prepare.py')
BUDGET = {'cpus': 1, 'memoryBytes': 2*imp.GIB, 'heapMiB': 128, 'offHeapMiB': 128,
          'maxSeconds': 60, 'workspaceMaxBytes': imp.GIB, 'reserveFreeBytes': 0,
          'maxLogBytes': 65536, 'pidsMax': 64}


class ReferenceImportContractTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.catalog, self.inventory, cache = fixtures.fixture(self.root)
        # Exercise the SF100 plan's structural contract using hand-authored unit records only.
        catalog = json.loads(self.catalog.read_text())
        inventory = json.loads(self.inventory.read_text())
        catalog['assets'][0]['scale'] = inventory['asset']['scale'] = '100'
        self.catalog.write_text(json.dumps(catalog))
        self.inventory.write_text(json.dumps(inventory))
        self.prepared = self.root/'prepared'
        fixtures.bridge.convert(self.catalog, self.inventory, cache, self.prepared,
                                1024**2, 1024**2, 0, 10)
        self.manifest = self.prepared/'reference-manifest.json'

    def facts(self):
        return imp.validate_manifest(self.manifest, self.catalog, self.inventory)

    def mutate(self, fn):
        value = json.loads(self.manifest.read_text())
        fn(value)
        self.manifest.write_text(json.dumps(value))

    def test_complete_table_coverage_and_regenerated_strict_arguments(self):
        facts = self.facts()
        self.assertEqual(len(facts['tables']), 31)
        self.assertEqual(facts['expected'], {'nodes': 8, 'relationships': 5})
        self.mutate(lambda value: value.update(referenceImportArguments=['--force', '--skip-bad-relationships=true']))
        plan = {'facts': self.facts(), 'budget': BUDGET}
        argv = imp.importer_arguments(plan)
        self.assertNotIn('--force', argv)
        self.assertIn('--bad-tolerance=0', argv)
        self.assertIn('--skip-bad-relationships=false', argv)
        self.assertEqual(sum(arg.startswith('--nodes=') for arg in argv), 8)
        self.assertEqual(sum(arg.startswith('--relationships=') for arg in argv), 23)

    def test_incomplete_manifest_cannot_be_planned(self):
        self.mutate(lambda value: value.update(status='incomplete'))
        with self.assertRaisesRegex(ValueError, 'Complete reference manifest'):
            self.facts()

    def test_missing_or_duplicate_csv_is_rejected(self):
        original = self.manifest.read_text()
        for mutation in [lambda v: v['files'].pop(), lambda v: v['files'].__setitem__(1, v['files'][0])]:
            self.manifest.write_text(original)
            self.mutate(mutation)
            with self.assertRaises(ValueError):
                self.facts()

    def test_counts_and_asset_identity_cannot_drift(self):
        original = self.manifest.read_text()
        for mutation in [lambda v: v['counts'].__setitem__('person', 2),
                         lambda v: v['files'][0].update(rows=123),
                         lambda v: v.update(memberCount=1),
                         lambda v: v['inputAsset'].update(sha256='0'*64)]:
            self.manifest.write_text(original)
            self.mutate(mutation)
            with self.assertRaises(ValueError):
                self.facts()

    def test_header_and_symlink_inputs_are_rejected(self):
        item = self.facts()['tables'][0]
        path = self.prepared/item['path']
        raw = path.read_bytes()
        path.write_bytes(b'X'+raw[1:])
        with self.assertRaisesRegex(ValueError, 'header'):
            self.facts()
        path.write_bytes(raw)
        alternate = path.with_suffix('.saved')
        path.rename(alternate)
        path.symlink_to(alternate.name)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.facts()

    def test_frozen_plan_includes_all_runtimes_and_cannot_overwrite(self):
        runtime = self.root/'runtime'
        launcher = runtime/'neo4j-community-4.4.24/bin/neo4j-admin'
        launcher.parent.mkdir(parents=True)
        launcher.write_text('unit fixture, never executed')
        lock = self.root/'runtime.json'
        imp.publish(lock, {'version': '4.4.24', 'edition': 'community',
                          'files': imp.tracked(runtime, [launcher])})
        java = self.root/'jdk'
        (java/'bin').mkdir(parents=True)
        (java/'bin/java').write_text('unit fixture, never executed')
        budget = self.root/'budget.json'
        imp.publish(budget, BUDGET)
        args = argparse.Namespace(prepared=self.manifest, catalog=self.catalog, inventory=self.inventory,
            runtime_lock=lock, runtime_root=runtime, java_home=java, budget=budget,
            workspace=self.root/'new-workspace', image='sha256:'+'a'*64)
        plan = imp.make_plan(args)
        path = self.root/'plan.json'
        imp.publish(path, plan)
        self.assertEqual(imp.load_plan(path), plan)
        self.assertFalse((self.root/'new-workspace').exists())
        with self.assertRaises(FileExistsError):
            imp.publish(path, plan)
        plan['budget']['maxSeconds'] += 1
        path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, 'identity'):
            imp.load_plan(path)
        (launcher.parent/'unexpected.jar').write_text('untracked')
        with self.assertRaisesRegex(ValueError, 'untracked'):
            imp.make_plan(args)


class ReferenceImportSupervisionTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()

    def test_native_memory_margin_and_boolean_budgets(self):
        self.assertEqual(imp.validate_budget(BUDGET), BUDGET)
        for changes in [{'memoryBytes': imp.GIB}, {'cpus': True}, {'maxSeconds': 0}]:
            with self.assertRaises(ValueError):
                imp.validate_budget({**BUDGET, **changes})

    def test_cgroup_failure_and_control_mismatch(self):
        observed = {'cpu.max': '100000 100000', 'memory.max': str(2*imp.GIB), 'memory.swap.max': '0',
                    'pids.max': '64', 'pids.events': {'max': 0}, 'memory.events': {'oom': 0, 'oom_kill': 0}}
        imp.check_controls(observed, BUDGET)
        for field, value in [('cpu.max', '200000 100000'), ('memory.swap.max', '1024'),
                             ('pids.events', {'max': 1}), ('memory.events', {'oom_kill': 1})]:
            with self.assertRaises((ValueError, RuntimeError)):
                imp.check_controls({**observed, field: value}, BUDGET)

    def test_runtime_mutation_and_extra_file_are_rejected(self):
        (self.root/'lib').write_bytes(b'locked')
        files = imp.tracked(self.root, [self.root/'lib'])
        imp.verify_tree(self.root, files, [])
        (self.root/'extra').write_bytes(b'new')
        with self.assertRaisesRegex(ValueError, 'inventory'):
            imp.verify_tree(self.root, files, [])
        (self.root/'extra').unlink()
        (self.root/'lib').write_bytes(b'change')
        with self.assertRaisesRegex(ValueError, 'Frozen file'):
            imp.verify_tree(self.root, files, [])

    def test_input_replacement_after_hash_is_detectable(self):
        path = self.root/'data.csv'
        path.write_bytes(b'original')
        files = imp.tracked(self.root, [path])
        before = imp.input_fingerprints(self.root, files)
        replacement = self.root/'replacement'
        replacement.write_bytes(b'changed!')
        replacement.replace(path)
        self.assertNotEqual(imp.input_fingerprints(self.root, files), before)

    def test_success_and_failure_exit_codes_are_preserved(self):
        for code in [0, 7]:
            work = self.root/str(code)
            work.mkdir()
            result = imp.supervise_process([sys.executable, '-c', f'print("evidence");raise SystemExit({code})'],
                                           work, 65536, lambda: None)
            self.assertEqual(result, code)
            self.assertEqual((work/'stdout.log').read_text(), 'evidence\n')

    def test_log_limit_stops_child_and_preserves_bounded_output(self):
        with self.assertRaisesRegex(RuntimeError, 'budget'):
            imp.supervise_process([sys.executable, '-c', 'import sys,time;sys.stdout.write("x"*1000000);sys.stdout.flush();time.sleep(60)'],
                                  self.root, 65536, lambda: None)
        self.assertLessEqual((self.root/'stdout.log').stat().st_size, 65536)
        pid = json.loads((self.root/'import-child.json').read_text())['pid']
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_guard_failure_before_dispatch_never_runs_child(self):
        with patch.object(imp.subprocess, 'Popen') as spawn:
            with self.assertRaises(TimeoutError):
                imp.supervise_process(['never'], self.root, 65536,
                                      lambda: (_ for _ in ()).throw(TimeoutError('deadline')))
            spawn.assert_not_called()

    def test_internal_worker_rejects_host_invocation_before_writes(self):
        with patch.object(imp, 'load_plan', return_value={'digest': 'a'*64}), patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, 'container context'):
                imp.container_worker(Path('/tmp/plan.json'))

    def test_exit_zero_without_receipt_remains_unknown(self):
        digest = 'a'*64
        imp.publish(self.root/'container.json', {'id': 'owned', 'planDigest': digest})
        info = {'Config': {'Labels': {'hgbench.owner': 'sf100-reference-import', 'hgbench.plan': digest,
                                     'hgbench.workspace': str(self.root)}},
                'State': {'Running': False, 'OOMKilled': False, 'ExitCode': 0, 'Status': 'exited'}}
        with patch.object(imp, 'load_plan', return_value={'digest': digest, 'workspace': str(self.root)}), \
                patch.object(imp, 'docker', return_value=json.dumps([info])):
            result = imp.inspect_attempt('plan.json')
        self.assertEqual(result['state'], 'interrupted-unknown')
        self.assertFalse(result['importProcessAccepted'])

    def test_completed_label_cannot_hide_bad_receipt_counts_or_failed_verification(self):
        digest = 'b'*64
        imp.publish(self.root/'container.json', {'id': 'owned', 'planDigest': digest})
        (self.root/'output').mkdir()
        plan = {'digest': digest, 'workspace': str(self.root), 'facts': {'expected': {'nodes': 8, 'relationships': 5}}}
        info = {'Config': {'Labels': {'hgbench.owner': 'sf100-reference-import', 'hgbench.plan': digest,
                                     'hgbench.workspace': str(self.root)}},
                'State': {'Running': False, 'OOMKilled': False, 'ExitCode': 0, 'Status': 'exited'}}
        receipt = {'schemaVersion': 1, 'kind': 'sf100-reference-import-receipt', 'planDigest': digest,
                   'state': 'process-completed-requires-database-validation', 'inputsFullyVerified': True,
                   'processExitCode': 0, 'error': None, 'reportedCounts': {'nodes': 8, 'relationships': 5}}
        for mutation, expected in [({}, True), ({'inputsFullyVerified': False}, False),
                                   ({'reportedCounts': {'nodes': 7, 'relationships': 5}}, False),
                                   ({'processExitCode': False}, False), ({'error': 'failure'}, False)]:
            (self.root/'output/receipt.json').write_text(json.dumps({**receipt, **mutation}))
            with patch.object(imp, 'load_plan', return_value=plan), patch.object(imp, 'docker', return_value=json.dumps([info])):
                self.assertEqual(imp.inspect_attempt('plan.json')['importProcessAccepted'], expected)

    @unittest.skipUnless(sys.platform == 'linux', 'The supervised Docker worker runs Linux; verified there')
    def test_deadline_cleans_up_descendant_after_leader_exit(self):
        started = time.monotonic()
        def guard():
            if time.monotonic()-started > 0.5:
                raise TimeoutError('deadline')
        pidfile = self.root/'grandchild.pid'
        script = ('import os,time; from pathlib import Path\n'
                  'p=os.fork()\n'
                  'if p:\n'
                  ' Path(' + repr(str(pidfile)) + ').write_text(str(p))\n'
                  ' os._exit(0)\n'
                  'time.sleep(60)\n')
        with self.assertRaises(TimeoutError):
            imp.supervise_process([sys.executable, '-c', script], self.root, 65536, guard)
        self.assertLess(time.monotonic()-started, 3)
        # A quick TimeoutError alone does not prove the orphan was killed.
        # Linux may briefly expose a dead zombie until the container init reaps it.
        descendant = int(pidfile.read_text())
        status = Path('/proc')/str(descendant)/'stat'
        live = False
        for _ in range(50):
            try: live = status.read_text().rsplit(')', 1)[1].split()[0] != 'Z'
            except FileNotFoundError: live = False
            if not live: break
            time.sleep(.01)
        self.assertFalse(live, 'Descendant remains live after supervised leader exit/deadline')

    def test_completion_counts_are_not_inferred_from_exit_code(self):
        text = 'Imported:\n 1,234 nodes\n 9,007,199,254,740,993 relationships\n 12 properties\n'
        self.assertEqual(imp.import_counts(text), {'nodes': 1234, 'relationships': 9007199254740993, 'properties': 12})
        self.assertIsNone(imp.import_counts('Process exited 0'))
        self.assertIsNone(imp.import_counts(text+text))


if __name__ == '__main__':
    unittest.main()
