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

"""Offline proof fixtures and failure gates only. Synthetic provenance is not a runtime qualification."""
import base64
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
from test_http_calibration_workflow import runtime as cohort_runtime

COMPONENT = Path(__file__).resolve().parents[1] / 'components/http/runtime-provenance.py'
SPEC = importlib.util.spec_from_file_location('runtime_provenance_test', COMPONENT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def spec():
    return {'schemaVersion': 1, 'kind': 'http-runtime-provenance-spec', 'sourceOrigin': 'linux-procfs',
        'budgets': {'wallMs': 60000, 'commandMs': 5000, 'handshakeMs': 10000, 'cleanupMs': 5000,
            'maxRawBytes': 262144, 'maxEvidenceBytes': 8 * MOD.MiB, 'maxExecutableBytes': 256 * MOD.MiB,
            'maxHashBytes': 768 * MOD.MiB},
        'container': {'cpus': .25, 'memoryBytes': 128 * MOD.MiB, 'pids': 32, 'tmpfsBytes': 8 * MOD.MiB}}


def stat_raw(pid, ticks):
    return str(pid) + ' (fixed (observer)) ' + ' '.join(['S'] + ['0'] * 18 + [str(ticks)] + ['0'] * 10) + '\n'


class Provenance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        MOD.CANCELLED = False
        self.value = spec()
        self.spec_file = self.base / 'spec.json'
        MOD.save(self.spec_file, self.value)
        runtime = cohort_runtime(self.base)['comparisonRuntime']
        runtime['hostPython'] = str(self.base / 'python')
        runtime['dockerExecutable'] = str(self.base / 'docker')
        node = Path(runtime['nodeHome']) / 'bin/node'
        node.parent.mkdir(parents=True)
        for path in (Path(runtime['hostPython']), Path(runtime['dockerExecutable']), node):
            path.write_bytes(('binary-fixture-' + path.name).encode())
        self.runtime = {'schemaVersion': 1, 'kind': 'http-comparison-runtime-plan', 'planDigest': 'a' * 64,
            'planContentDigest': 'b' * 64, 'componentSha256': 'c' * 64, 'settings': runtime,
            'identities': {'hostPython': MOD.COMMON.file_entry(runtime['hostPython']),
                'dockerExecutable': MOD.COMMON.file_entry(runtime['dockerExecutable']),
                'node': [{'path': 'bin/node', 'type': 'file', **{key: value for key, value in
                    MOD.COMMON.file_entry(node).items() if key != 'path'}}], 'source': []}}
        self.selection = self.base / 'selection'
        self.selection.mkdir()
        self.runtime_path = self.selection / 'runtime.json'
        MOD.save(self.runtime_path, self.runtime)
        MOD.save(self.selection / 'runtime.lock.json', {'schemaVersion': 1, 'kind': 'http-comparison-runtime-lock',
            'planDigest': 'a' * 64, 'runtime': MOD.COMMON.file_entry(self.runtime_path, 'runtime.json')})

    def freeze(self):
        self.root, self.plan = MOD.freeze(self.spec_file, self.runtime_path, self.base / 'proof', MOD.Budget(self.value))
        self.plan_sha = MOD.COMMON.file_entry(self.root / 'plan.json')['sha256']
        return self.root

    def seal(self):
        lock = self.root / 'raw.lock.json'
        if lock.exists():
            lock.unlink()
        files = [MOD.COMMON.file_entry(path, path.relative_to(self.root).as_posix())
            for path in MOD.COMMON.regular_files(self.root)
            if path.name not in ('runtime-binding.json', 'external-expectations.json') and
               not path.relative_to(self.root).as_posix().startswith('recovery/')]
        MOD.save(lock, {'schemaVersion': 1, 'kind': 'http-runtime-provenance-raw-lock', 'files': files,
                       'contentDigest': MOD.value_hash(files)})

    def container(self, running=False):
        plan, limits = self.plan, self.value['container']
        return {'Id': 'd' * 64, 'Name': '/' + plan['containerName'], 'Image': plan['binding']['imageId'],
            'State': {'Running': running, 'Pid': 1003 if running else 0, 'ExitCode': 0,
                'Paused': False, 'Restarting': False, 'OOMKilled': False, 'Dead': False},
            'Config': {'User': str(plan['owner']['uid']) + ':' + str(plan['owner']['gid']), 'WorkingDir': '/tmp',
                'Entrypoint': [plan['runtimeSettings']['driver']['python']],
                'Cmd': MOD.worker_argv(plan, self.root, 'driver-python')[1:],
                'Labels': {MOD.LABEL + '.attempt': plan['attemptId'], MOD.LABEL + '.plan': self.plan_sha},
                'injectionEnvironmentPresent': {key: False for key in MOD.FLAGS}},
            'HostConfig': {'NetworkMode': 'none', 'ReadonlyRootfs': True, 'Memory': limits['memoryBytes'],
                'MemorySwap': limits['memoryBytes'], 'NanoCpus': 250000000, 'PidsLimit': limits['pids'],
                'CapDrop': ['ALL'], 'CapAdd': [], 'Privileged': False, 'SecurityOpt': ['no-new-privileges:true'],
                'RestartPolicy': {'Name': 'no'}, 'PidMode': '', 'Devices': [], 'VolumesFrom': [],
                'LogConfig': {'Type': 'none'}, 'PortBindings': {}, 'PublishAllPorts': False,
                'Tmpfs': {'/tmp': 'rw,noexec,nosuid,nodev,size=' + str(limits['tmpfsBytes']) + ',mode=1777'}},
            'Mounts': [{'Type': 'bind', 'Source': str(self.root / 'control'), 'Destination': '/probe', 'RW': False}]}

    def role(self, role):
        index = MOD.ROLES.index(role)
        host_pid, pid = 1001 + index, 1 if role == 'driver-python' else 1001 + index
        request = MOD.read_json(self.root / 'control' / (role + '.json'))
        expected = self.plan['binding']['selected'].get(role, {'bytes': 17, 'sha256': 'e' * 64})
        executable = {'bytes': expected['bytes'], 'sha256': expected['sha256'], 'device': '1', 'inode': str(20 + index),
                      'mtimeNs': '100', 'ctimeNs': '101'}
        ns = {name: ('time' if name == 'time_for_children' else name) + ':[401]' for name in MOD.NS}
        boot = '11111111-2222-3333-4444-555555555555'
        offsets = 'monotonic 0 0\nboottime 0 0\n'
        runtime = {'name': 'python', 'version': '3.12.15' if role == 'driver-python' else '3.12.3',
            'implementation': 'cpython', 'versionText': '3.12.15 fixture' if role == 'driver-python' else '3.12.3 fixture',
            'reportedExecutable': self.plan['runtimeSettings']['driver']['python'] if role == 'driver-python' else
                self.plan['runtimeSettings']['hostPython'],
            'flags': {'isolated': 1, 'ignore_environment': 1, 'no_site': 1, 'no_user_site': 1, 'dont_write_bytecode': 1},
            'injectionEnvironmentPresent': {key: False for key in MOD.FLAGS}}
        clock = {'api': 'time.monotonic_ns', 'implementation': 'clock_gettime(CLOCK_MONOTONIC)',
                 'monotonic': True, 'adjustable': False, 'resolutionSeconds': 1e-9}
        if role == 'host-node':
            runtime = {'name': 'node', 'version': 'v22.14.0', 'uv': '1.49.2', 'platform': 'linux', 'arch': 'x64',
                'nodeSharedLibuv': False, 'reportedExecutable': str(Path(self.runtime['settings']['nodeHome']) / 'bin/node'),
                'execArgvSha256': MOD.value_hash([]), 'injectionEnvironmentPresent': {key: False for key in MOD.FLAGS}}
            clock = {'api': 'process.hrtime.bigint', 'nowNs': '1000000000',
                     'performanceNowMs': 1, 'performanceTimeOriginMs': 100}
        identity = {'pid': pid, 'parentPid': 0 if role == 'driver-python' else 77, 'statRaw': stat_raw(pid, 99 + index),
            'startTimeTicks': str(99 + index), 'bootId': boot, 'namespaces': ns, 'timeOffsetsRaw': offsets,
            'runtime': runtime, 'clock': clock}
        argv = MOD.worker_argv(self.plan, self.root, role)
        parent = {'pid': host_pid, 'statBeforeRaw': stat_raw(host_pid, 99 + index),
            'statAfterRaw': stat_raw(host_pid, 99 + index), 'executable': {'path': '/proc/' + str(host_pid) + '/exe',
            **executable}, 'namespaces': ns, 'timeOffsetsRaw': offsets,
            'cmdlineBase64': base64.b64encode(b'\0'.join(value.encode() for value in argv) + b'\0').decode(),
            'statusRaw': 'NSpid:\t' + str(host_pid) + ('\t1' if role == 'driver-python' else '') + '\nUid:\t' +
                '\t'.join([str(self.plan['owner']['uid'])] * 4) + '\n',
            'bootId': boot, 'bootIdRaw': boot + '\n', 'observedMonotonicNs': '1000000000'}
        ready = {'kind': 'ready', 'request': request, 'identity': identity, 'executable': executable}
        final = {'kind': 'final', 'request': request, 'identity': copy.deepcopy(identity),
                 'executable': {key: value for key, value in executable.items() if key != 'sha256'}}
        frames = [json.dumps(frame) + '\n' for frame in (ready, final)]
        if role == 'driver-python':
            argv = [self.runtime['settings']['dockerExecutable'], '--host', 'unix:///var/run/docker.sock',
                    'start', '--attach', '--interactive', 'd' * 64]
        return {'role': role, 'status': 'completed', 'childExitCode': 0, 'stderrBytes': 0,
            'stdoutBytes': sum(len(raw.encode()) for raw in frames), 'frames': frames, 'ready': ready, 'final': final,
            'before': parent, 'after': copy.deepcopy(parent), 'argv': argv,
            'actualPidSource': 'docker-inspect-State.Pid' if role == 'driver-python' else 'owned-Popen.pid'}

    def complete(self):
        self.freeze()
        image = {'Id': self.plan['binding']['imageId'], 'Os': 'linux', 'Architecture': 'amd64',
                 'injectionEnvironmentPresent': {key: False for key in MOD.FLAGS}}
        created, live = self.container(), self.container(True)
        for name, data in [('image.json', image), ('container-created.json', created), ('container-live.json', live)]:
            MOD.save(self.root / name, data)
        (self.root / 'container.id').write_text('d' * 64)
        docker = MOD.Docker(self.root, self.plan, MOD.Budget(self.value))
        prefix = [self.runtime['settings']['dockerExecutable'], '--host', 'unix:///var/run/docker.sock']
        commands = [(['image', 'inspect', image['Id']], image, False), (docker.create_args(), None, False),
            (['inspect', '--type', 'container', 'd' * 64], created, False),
            (['inspect', '--type', 'container', 'd' * 64], live, False),
            (['inspect', '--type', 'container', 'd' * 64], created, False),
            (['rm', '-f', 'd' * 64], None, False), (['inspect', '--type', 'container', 'd' * 64], None, True)]
        for number, (argv, inspection, missing) in enumerate(commands, 1):
            data = {'argv': prefix + argv, 'exitCode': 1 if missing else 0, 'error': None, 'notFound': missing,
                    'stdoutBytes': 10, 'stderrBytes': 0}
            if inspection:
                data['inspection'] = inspection
            MOD.save(self.root / 'docker' / ('command-' + str(number).zfill(3) + '.json'), data)
        selected = {'docker': {'path': self.runtime['settings']['dockerExecutable'],
                    **self.runtime['identities']['dockerExecutable']}}
        records = {role: self.role(role) for role in MOD.ROLES}
        for role, record in records.items():
            MOD.save(self.root / 'roles' / (role + '.json'), record)
            if role != 'driver-python':
                selected[role] = record['before']['executable']
        MOD.save(self.root / 'selected-files-before.json', selected)
        MOD.save(self.root / 'selected-files-after.json', selected)
        raw_bytes = 10 * len(commands)
        for record in records.values():
            raw_bytes += record['stdoutBytes']
            for parent in (record['before'], record['after']):
                raw_bytes += sum(len(parent[key].encode()) for key in ('statBeforeRaw', 'statAfterRaw',
                    'timeOffsetsRaw', 'statusRaw', 'bootIdRaw')) + len(base64.b64decode(parent['cmdlineBase64']))
        hash_bytes = 2 * sum(item['bytes'] for item in selected.values()) + 3 * sum(
            record['ready']['executable']['bytes'] for record in records.values())
        MOD.save(self.root / 'dispatch.json', {'attemptId': self.plan['attemptId']})
        MOD.save(self.root / 'result.json', {'schemaVersion': 1, 'kind': 'http-runtime-provenance-result',
            'attemptId': self.plan['attemptId'], 'status': 'completed', 'failure': None,
            'cleanup': {'containerRemoved': True, 'containerId': 'd' * 64},
            'roles': {role: 'completed' for role in MOD.ROLES}, 'rawBytes': raw_bytes, 'hashBytes': hash_bytes})
        self.seal()
        return self.root

    def test_strict_spec_rejects_hooks_nonfinite_and_inconsistent_budgets(self):
        MOD.validate_spec(self.value)
        for mutate in [lambda value: value.update(command=['sh']), lambda value: value['container'].update(cpus=float('nan')),
                       lambda value: value['budgets'].update(handshakeMs=20000, wallMs=5000),
                       lambda value: value.update(sourceOrigin='fixture')]:
            value = copy.deepcopy(self.value)
            mutate(value)
            with self.assertRaises(MOD.COMMON.WorkflowError):
                MOD.validate_spec(value)

    def test_runtime_lock_and_selected_binary_anchors(self):
        _, _, binding = MOD.runtime_binding(self.runtime_path)
        self.assertEqual(binding['imageId'], 'sha256:' + 'a' * 64)
        self.runtime_path.write_text('{}')
        with self.assertRaises(MOD.COMMON.WorkflowError):
            MOD.runtime_binding(self.runtime_path)

    def test_complete_raw_proof_rebuilds_distinct_python_versions_and_strict_binding(self):
        result = MOD.verify(self.complete())
        self.assertTrue(result['valid'])
        binding = result['runtimeBinding']
        self.assertEqual(binding['driver']['pythonVersion'], '3.12.15')
        self.assertEqual(result['externalExpectations']['hostPythonVersion'], '3.12.3')
        self.assertEqual(binding['proofDigest'], MOD.read_json(self.root / 'raw.lock.json')['contentDigest'])
        self.assertEqual(result['externalExpectations']['probedRuntimeDigest'], self.plan['binding']['runtimeDigest'])
        self.assertEqual(result['clockAlignment'], 'unverified')
        self.assertFalse(result['performanceAccepted'])
        self.assertEqual(set(binding), {'schemaVersion', 'kind', 'sourceOrigin', 'runtimeDigest', 'driver', 'node', 'proofDigest'})

    def test_relocated_proof_rebuilds_identical_binding_without_host_access(self):
        self.complete()
        first = MOD.verify(self.root)
        moved = self.base / 'relocated'
        shutil.copytree(self.root, moved)
        shutil.rmtree(self.root)
        self.assertEqual(MOD.verify(moved), first)

    def test_semantic_forgery_rehashed_lock_cannot_change_actual_proc_image_or_source(self):
        self.complete()
        path = self.root / 'roles/driver-python.json'
        original = MOD.read_json(path)
        for mutation in ['proc-sha', 'ticks', 'namespace', 'injection', 'pid-source', 'version']:
            record = copy.deepcopy(original)
            if mutation == 'proc-sha':
                record['before']['executable']['sha256'] = 'f' * 64
            elif mutation == 'ticks':
                record['before']['statAfterRaw'] = stat_raw(1003, 999)
            elif mutation == 'namespace':
                record['after']['namespaces']['time'] = 'time:[999]'
            elif mutation == 'injection':
                record['ready']['identity']['runtime']['injectionEnvironmentPresent']['LD_PRELOAD'] = True
            elif mutation == 'pid-source':
                record['actualPidSource'] = 'self-report'
            else:
                record['final']['identity']['runtime']['version'] = '3.11.1'
            if mutation in ('injection', 'version'):
                record['frames'] = [json.dumps(record[key]) + '\n' for key in ('ready', 'final')]
                record['stdoutBytes'] = sum(len(raw.encode()) for raw in record['frames'])
            MOD.save(path, record, replace=True)
            self.seal()
            with self.assertRaises(MOD.COMMON.WorkflowError, msg=mutation):
                MOD.verify(self.root)
        MOD.save(path, original, replace=True)
        live = MOD.read_json(self.root / 'container-live.json')
        live['Image'] = 'sha256:' + 'f' * 64
        MOD.save(self.root / 'container-live.json', live, replace=True)
        self.seal()
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'CONTAINER_IDENTITY'):
            MOD.verify(self.root)

    def test_summary_booleans_and_missing_cleanup_cannot_qualify_proof(self):
        self.complete()
        result = MOD.read_json(self.root / 'result.json')
        result['cleanup']['containerRemoved'] = False
        result['verified'] = True
        MOD.save(self.root / 'result.json', result, replace=True)
        self.seal()
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'RESULT_INCOMPLETE'):
            MOD.verify(self.root)

    def test_resealed_zero_raw_or_hash_totals_are_rejected_by_independent_accounting(self):
        self.complete()
        original = MOD.read_json(self.root / 'result.json')
        for field in ('rawBytes', 'hashBytes'):
            result = copy.deepcopy(original)
            result[field] = 0
            MOD.save(self.root / 'result.json', result, replace=True)
            self.seal()
            with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'RAW_HASH_ACCOUNTING'):
                MOD.verify(self.root)

    def test_extra_files_and_source_mutation_rejected(self):
        self.complete()
        (self.root / 'unexpected.txt').write_text('extra')
        self.seal()
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'UNKNOWN_EVIDENCE'):
            MOD.verify(self.root)
        (self.root / 'unexpected.txt').unlink()
        (self.root / 'control/runtime-provenance-worker.py').write_text('altered')
        self.seal()
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'SOURCE_CHANGED'):
            MOD.verify(self.root)

    def test_docker_receipts_redact_environment_values_and_nonprobe_labels(self):
        self.freeze()
        value = self.container()
        value['Config']['Env'] = ['LD_PRELOAD=private-secret', 'TOKEN=another-secret']
        value['Config']['Labels']['unrelated.label'] = 'label-secret'
        safe = MOD.safe_container(value)
        self.assertNotIn('private-secret', json.dumps(safe))
        self.assertNotIn('another-secret', json.dumps(safe))
        self.assertNotIn('label-secret', json.dumps(safe))
        self.assertTrue(safe['Config']['injectionEnvironmentPresent']['LD_PRELOAD'])

    def test_cleanup_rejects_replaced_cid_without_stopping_anything(self):
        self.freeze()
        (self.root / 'container.id').write_text('d' * 64)
        docker = MOD.Docker(self.root, self.plan, MOD.Budget(self.value))
        foreign = self.container()
        foreign['Id'] = 'e' * 64
        with patch.object(docker, 'call', return_value=(foreign, {'notFound': False})) as call:
            with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'CONTAINER_REPLACED'):
                docker.cleanup()
        self.assertEqual(call.call_count, 1)
        self.assertEqual(call.call_args.args[0][0], 'inspect')

    def test_failed_proof_preserves_missing_roles_without_exporting_expectations(self):
        self.freeze()
        MOD.save(self.root / 'result.json', {'schemaVersion': 1, 'kind': 'http-runtime-provenance-result',
            'attemptId': self.plan['attemptId'], 'status': 'failed', 'failure': 'fixture-denied',
            'cleanup': {'containerRemoved': False}, 'roles': {}, 'rawBytes': 0, 'hashBytes': 0})
        self.seal()
        result = MOD.verify(self.root)
        self.assertFalse(result['valid'])
        self.assertFalse(result['expectationsAvailable'])
        self.assertNotIn('runtimeBinding', result)

    def test_probe_selected_file_drift_never_launches_observers_and_retains_failure(self):
        Path(self.runtime['settings']['hostPython']).write_text('changed executable')
        with patch.object(MOD.sys, 'platform', 'linux'), patch.object(MOD.os, 'geteuid', return_value=1000), \
                patch.object(MOD.Docker, 'cleanup', return_value={'containerRemoved': True}), \
                patch.object(MOD, 'collect_role', side_effect=AssertionError('must not launch')):
            result = MOD.probe(self.spec_file, self.runtime_path, self.base / 'proof')
        self.assertFalse(result['valid'])
        self.assertTrue((self.base / 'proof/raw.lock.json').exists())
        self.assertFalse((self.base / 'proof/external-expectations.json').exists())

    def test_stop_is_durable_before_container_creation_and_validates_identity(self):
        with patch.object(MOD.os, 'geteuid', return_value=1000):
            self.freeze()
        with patch.object(MOD.sys, 'platform', 'linux'), patch.object(MOD.os, 'geteuid', return_value=1000), \
                patch.object(MOD.Docker, 'cleanup', return_value={'containerRemoved': True}):
            result = MOD.stop(self.root)
            MOD.stop(self.root)
        self.assertTrue(result['cleanup']['containerRemoved'])
        marker = MOD.read_json(self.root / 'stop-request.json')
        budget = MOD.Budget(self.value)
        budget.stop_path, budget.stop_binding = self.root / 'stop-request.json', marker
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'STOP_REQUESTED'):
            budget.check()
        MOD.save(self.root / 'stop-request.json', {'attemptId': 'foreign'}, replace=True)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'STOP_IDENTITY'):
            budget.check()

    def test_observer_drip_deadline_is_absolute_and_child_is_reaped(self):
        value = spec()
        value['budgets'].update(handshakeMs=500, wallMs=5000)
        observer = MOD.Observer([sys.executable, '-c',
            'import os,time\nwhile True:\n os.write(1,b"x"); time.sleep(.02)'], MOD.Budget(value))
        begun = time.monotonic()
        try:
            with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'HANDSHAKE_DEADLINE'):
                observer.receive()
        finally:
            observer.close()
        self.assertLess(time.monotonic() - begun, 2)
        self.assertIsNotNone(observer.child.poll())

    def test_observer_frame_and_duplicate_key_limits_reject_without_unbounded_buffer(self):
        for script in ['import os; os.write(1,b"x"*40000)',
                       'import os; os.write(1,b\'{"kind":1,"kind":2}\\n\')']:
            observer = MOD.Observer([sys.executable, '-c', script], MOD.Budget(spec()))
            try:
                with self.assertRaises((MOD.COMMON.WorkflowError, ValueError)):
                    observer.receive()
            finally:
                observer.close()
            self.assertIsNotNone(observer.child.poll())

    def test_cleanup_raw_reserve_and_hash_limit_are_independent(self):
        budget = MOD.Budget(spec())
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'RAW_BUDGET'):
            budget.raw(self.value['budgets']['maxRawBytes'] - 32768 + 1)
        budget.raw_bytes = self.value['budgets']['maxRawBytes'] - 32768
        budget.cleanup_deadline = time.monotonic() + 5
        budget.raw(1000)
        with self.assertRaisesRegex(MOD.COMMON.WorkflowError, 'HASH_BUDGET'):
            budget.hash(self.value['budgets']['maxHashBytes'] + 1)

    @unittest.skipUnless(shutil.which('node'), 'public CLI requires the workspace Node runtime')
    def test_actual_public_cli_describe_and_offline_verify_rebuild_contract(self):
        self.complete()
        cli = COMPONENT.parents[2] / 'dist/cli.js'
        env = dict(os.environ, HGBENCH_PYTHON=sys.executable)
        def invoke(args):
            child = subprocess.run([shutil.which('node'), str(cli), 'http', 'runtime', *args],
                capture_output=True, timeout=10, env=env)
            self.assertEqual(child.returncode, 0, child.stdout.decode() + child.stderr.decode())
            self.assertLess(len(child.stdout), MOD.MiB)
            return json.loads(child.stdout)['data']
        self.assertEqual(invoke(['describe'])['kind'], 'http-runtime-provenance-capability')
        result = invoke(['verify', str(self.root)])
        self.assertTrue(result['valid'])
        self.assertEqual(result['runtimeBinding'], MOD.verify(self.root)['runtimeBinding'])

    @unittest.skipUnless(sys.platform == 'linux', 'actual /proc observation requires Linux')
    def test_actual_fixed_host_python_and_node_workers_bind_to_external_proc_fd(self):
        self.freeze()
        actual_node = shutil.which('node')
        roles = ['host-python'] + (['host-node'] if actual_node else [])
        for role in roles:
            plan = copy.deepcopy(self.plan)
            if role == 'host-python':
                selected = str(Path(sys.executable).resolve())
                plan['runtimeSettings']['hostPython'] = selected
            else:
                # Host Node layout is fixed; a private temp bin/node symlink selects the actual test Node.
                selected = str(Path(actual_node).resolve())
                fake_node = Path(plan['runtimeSettings']['nodeHome']) / 'bin/node'
                fake_node.unlink()
                fake_node.symlink_to(selected)
            plan['binding']['selected'][role] = MOD.COMMON.file_entry(selected)
            record = MOD.collect_role(self.root, plan, role, MOD.Budget(spec()), None)
            observed = MOD.validate_role(record, plan, self.root, role)
            self.assertEqual(observed['executableSha256'], MOD.COMMON.file_entry(selected)['sha256'])


if __name__ == '__main__':
    unittest.main()
