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

import copy
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import selectors
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('http_environment', ROOT/'components/http/hugegraph-environment.py')
ENV = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENV)
SERVICE = ENV.SERVICE
OBSERVER = ENV.observation_component()


def observation_spec(maximum=65536):
    return {'schemaVersion': 1, 'kind': 'http-jvm-observation-spec', 'preset': 'hotspot17-gc-jit-v1',
            'capture': 'supervisor-stdout', 'maxRawBytes': maximum, 'windowMs': 1000,
            'parser': {'maxLineBytes': 65536, 'maxEvents': 10000, 'maxWindows': 1000,
                       'maxOutputBytes': maximum + 2*1024**2, 'timeoutMs': 10000}}


def settings(root):
    dist, jdk = root/'distribution', root/'jdk'
    for path, value in {
        dist/'bin/jvm-module.options': '--add-modules=jdk.unsupported\n',
        dist/'conf/gremlin-server.yaml': 'evaluationTimeout: 30000\ngraphs: {}\n',
        dist/'conf/java-security.properties': 'networkaddress.cache.ttl=30\n',
        dist/'scripts/empty-sample.groovy': '// empty\n',
        dist/'lib/hugegraph-core.jar': 'fixture-jar-identity',
        dist/'lib/other.jar': 'dependency-identity',
        dist/'lib/log4j-slf4j-impl-fixture.jar': 'log4j-binding-identity',
        dist/'ext/extra.jar': 'server-extension-identity',
        jdk/'release': 'JAVA_VERSION="17.0.17"\n',
        jdk/'bin/java': 'fixture-java-identity',
        jdk/'bin/jcmd': 'fixture-jcmd-identity',
        jdk/'conf/security/java.security': '# fixture\n',
    }.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    (jdk/'bin/java').chmod(0o755)
    (jdk/'bin/jcmd').chmod(0o755)
    return {'schemaVersion': 1, 'kind': 'hugegraph-http-environment-settings', 'purpose': 'functional',
            'distribution': str(dist), 'javaHome': str(jdk), 'image': 'sha256:' + 'a'*64,
            'python': '/usr/bin/python3', 'dockerSocket': '/var/run/docker.sock', 'mode': 'forward',
            'budget': {'cpus': 2, 'memoryBytes': 2*1024**3, 'pids': 512, 'wallSeconds': 10,
                       'startupSeconds': 5, 'shutdownSeconds': 1, 'maxAllocatedBytes': 32*1024**2,
                       'maxLogicalBytes': 32*1024**2, 'maxLogBytes': 65536, 'minFreeBytes': 0,
                       'tmpfsBytes': 32*1024**2, 'pollSeconds': 0.1},
            'jvm': {'heapMinBytes': 128*1024**2, 'heapMaxBytes': 512*1024**2,
                    'maxDirectMemoryBytes': 128*1024**2, 'activeProcessorCount': 2},
            'server': {'gremlinPool': 4, 'bossThreads': 1, 'workerThreads': 4, 'maxWorkQueueSize': 128,
                       'httpWorkerThreads': 8, 'maxRoute': 8, 'timeoutSeconds': 30, 'idleTimeoutSeconds': 30,
                       'connectionMaxRequests': 10000, 'maxContentLengthBytes': 65536,
                       'resultIterationBatchSize': 64}}


def inspected(plan, plan_sha):
    s, b = plan['settings'], plan['settings']['budget']
    mounts = [(s['distribution'], '/opt/hugegraph', False), (s['javaHome'], '/opt/jdk', False),
              (plan['root'], '/control', False), (plan['root']+'/work', '/work', True),
              (s['distribution']+'/scripts', '/work/scripts', False)]
    return {'Id': 'b'*64, 'Name': '/'+plan['containerName'], 'Image': s['image'],
            'Config': {'Labels': {ENV.LABEL+'.attempt': plan['attemptId'], ENV.LABEL+'.plan': plan_sha},
                       'User': f"{plan['owner']['uid']}:{plan['owner']['gid']}",
                       'WorkingDir': '/work', 'Entrypoint': [s['python']],
                       'Cmd': ['-B', '/control/hugegraph-service.py', '--plan', '/control/plan.json',
                               '--plan-sha256', plan_sha, '--work', '/work']},
            'HostConfig': {'NetworkMode': 'none', 'PortBindings': {}, 'ReadonlyRootfs': True,
                           'Memory': b['memoryBytes'], 'MemorySwap': b['memoryBytes'],
                           'NanoCpus': round(b['cpus']*1000000000), 'PidsLimit': b['pids'],
                           'CapDrop': ['ALL'], 'CapAdd': [], 'Privileged': False,
                           'SecurityOpt': ['no-new-privileges:true'], 'RestartPolicy': {'Name': 'no'},
                           'LogConfig': {'Type': 'none'}, 'PidMode': '', 'Devices': [], 'VolumesFrom': [],
                           'Tmpfs': {'/tmp': f"rw,exec,nosuid,nodev,size={b['tmpfsBytes']},mode=1777"}},
            'NetworkSettings': {'Ports': {}},
            'Mounts': [{'Type': 'bind', 'Source': src, 'Destination': dst, 'RW': rw} for src, dst, rw in mounts],
            'State': {'Status': 'created', 'Running': False, 'ExitCode': 0, 'OOMKilled': False}}


class DockerStub:
    """No Docker commands run: controlled responses exercise host orchestration."""
    def __init__(self, root, plan, sha, fail_start=False):
        self.root, self.plan, self.sha = root, plan, sha
        self.info = None
        self.calls = []
        self.fail_start = fail_start

    def __call__(self, plan, *args, check=True):
        self.calls.append(args)
        if args[:2] == ('image', 'inspect'):
            value = [{'Id': plan['settings']['image']}]
        elif args[0] == 'create':
            self.info = inspected(plan, self.sha)
            (self.root/'container.id').write_text(self.info['Id']+'\n')
            return subprocess.CompletedProcess(args, 0, (self.info['Id']+'\n').encode(), b'')
        elif args[0] == 'inspect':
            if self.info is None:
                return subprocess.CompletedProcess(args, 1, b'', b'Error: No such object')
            value = [self.info]
        elif args[0] == 'start':
            self.info['State'].update(Running=True, Status='running')
            if self.fail_start:
                raise SERVICE.EnvironmentError('injected-start-response-failure')
            identity = {'attemptId': plan['attemptId'], 'planSha256': self.sha}
            SERVICE.write_json(self.root/'work/ready.json', dict(identity, actualModeVerified=False))
            SERVICE.write_json(self.root/'work/service-result.json', dict(identity, error=None,
                               outcomes=[{'reason': 'stop-requested', 'termination': {'termSent': True,
                                          'killSent': False, 'exitCode': 143, 'cleanupComplete': True}}]))
            self.info['State'].update(Running=False, Status='exited')
            value = {}
        elif args[0] == 'stop':
            assert args[-1] == self.info['Id']
            self.info['State'].update(Running=False, Status='exited')
            value = {}
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(args, 0, json.dumps(value).encode(), b'')


class HttpEnvironment(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hgbench-env-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = settings(self.root)
        # Planning must reject actual root execution. These offline fixtures exercise nonroot plans on root CI too.
        self.uid = patch.object(ENV.os, 'geteuid', return_value=1001)
        self.gid = patch.object(ENV.os, 'getegid', return_value=1001)
        self.uid.start()
        self.gid.start()
        self.addCleanup(self.uid.stop)
        self.addCleanup(self.gid.stop)

    def planned(self, name='attempt'):
        file = self.root/(name+'-settings.json')
        SERVICE.write_json(file, self.settings)
        result = ENV.freeze(file, self.root/name)
        root, plan, sha = ENV.load_attempt(result['attempt'], verify_sources=True)
        return root, plan, sha

    def test_offline_freeze_is_exclusive_and_modes_change_only_one_config_value(self):
        first, plan, _ = self.planned('forward')
        self.settings['mode'] = 'embedded'
        second, other, _ = self.planned('embedded')
        self.assertEqual(plan['sources'], other['sources'])
        self.assertEqual(SERVICE.java_commands(plan), SERVICE.java_commands(other))
        for relative in ('gremlin-server.yaml', 'log4j2.xml', 'graphs/hugegraph.properties',
                         'java-security.properties'):
            self.assertEqual((first/'conf'/relative).read_bytes(), (second/'conf'/relative).read_bytes())
        self.assertEqual((first/'conf/rest-server.properties').read_text().replace('=forward\n', '=embedded\n'),
                         (second/'conf/rest-server.properties').read_text())
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'already-exists'):
            ENV.freeze(self.root/'forward-settings.json', first)
        with patch.object(ENV, 'docker', side_effect=AssertionError('offline status must not call Docker')):
            self.assertEqual(ENV.status(first)['state'], 'planned')

    def test_source_config_and_component_changes_reject_start_but_preserve_cleanup_identity(self):
        root, plan, _ = self.planned()
        for path in (Path(plan['settings']['distribution'])/'lib/other.jar',
                     Path(plan['settings']['javaHome'])/'bin/java', root/'conf/rest-server.properties',
                     root/'hugegraph-service.py'):
            original = path.read_bytes()
            path.write_bytes(original+b'changed')
            with self.subTest(path=str(path)):
                with self.assertRaisesRegex(SERVICE.EnvironmentError, 'content-changed'):
                    ENV.load_attempt(root, verify_sources=True)
                # Cleanup consumes plan/identity, never mutable build/config contents.
                self.assertEqual(ENV.load_attempt(root)[1]['attemptId'], plan['attemptId'])
            path.write_bytes(original)

    def test_exact_owner_plan_and_source_symlink_identity(self):
        root, _, _ = self.planned()
        with patch.object(ENV.os, 'geteuid', return_value=1002):
            with self.assertRaisesRegex(SERVICE.EnvironmentError, 'owner-mismatch'):
                ENV.load_attempt(root)
        (self.root/'outside').write_text('external')
        (self.root/'jdk/escape').symlink_to('../outside')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'symlink-escapes'):
            SERVICE.inventory(self.root/'jdk')
        with (root/'plan.json').open('a') as stream:
            stream.write(' ')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'plan-identity'):
            ENV.load_attempt(root)

    def test_invalid_settings_and_dangerous_mount_paths_are_rejected(self):
        for key, value in [('mode', 'typo'), ('purpose', 'formal'), ('image', 'apache/hugegraph:latest'),
                           ('python', '/bin/python,other'), ('distribution', 'relative')]:
            changed = copy.deepcopy(self.settings)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(SERVICE.EnvironmentError):
                ENV.validate_settings(changed)
        for key, value in [('memoryBytes', 128*1024**2), ('pids', 0), ('wallSeconds', 0), ('cpus', float('nan'))]:
            changed = copy.deepcopy(self.settings)
            changed['budget'][key] = value
            with self.subTest(key=key), self.assertRaises(SERVICE.EnvironmentError):
                ENV.validate_settings(changed)

    def test_docker_argv_has_no_shell_and_enforces_isolation_and_exact_resources(self):
        _, plan, sha = self.planned()
        args = ENV.create_arguments(plan, sha)
        self.assertNotIn('sh', args)
        self.assertNotIn('bash', args)
        self.assertNotIn('--publish', args)
        self.assertEqual(args[args.index('--network')+1], 'none')
        self.assertEqual(args[args.index('--memory')+1], args[args.index('--memory-swap')+1])
        self.assertIn('--read-only', args)
        self.assertEqual(args[args.index('--log-driver')+1], 'none')
        info = inspected(plan, sha)
        info['NetworkSettings']['Ports'] = {'18080/tcp': None}  # EXPOSE metadata does not publish a host port.
        ENV.verify_container(info, plan, sha, 'b'*64)
        ENV.verify_limits(info, plan)
        for change in ('image', 'owner', 'memory', 'network', 'tmpfs', 'command', 'mount'):
            broken = copy.deepcopy(info)
            if change == 'image':
                broken['Image'] = 'sha256:'+'c'*64
            elif change == 'owner':
                broken['Config']['Labels'][ENV.LABEL+'.attempt'] = 'other'
            elif change == 'memory':
                broken['HostConfig']['MemorySwap'] += 1
            elif change == 'network':
                broken['HostConfig']['NetworkMode'] = 'host'
            elif change == 'tmpfs':
                broken['HostConfig']['Tmpfs'] = {}
            elif change == 'command':
                broken['Config']['Cmd'] = ['different']
            else:
                broken['Mounts'][0]['RW'] = True
            with self.subTest(change=change), self.assertRaises(SERVICE.EnvironmentError):
                ENV.verify_container(broken, plan, sha, 'b'*64)
                ENV.verify_limits(broken, plan)
        with patch.object(ENV.shutil, 'which', return_value='/usr/bin/docker'), \
                patch.object(ENV.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, b'{}', b'')) as run:
            ENV.docker(plan, 'inspect', 'b'*64)
            self.assertIsInstance(run.call_args.args[0], list)
            self.assertNotIn('shell', run.call_args.kwargs)
            self.assertEqual(run.call_args.args[0][1:3], ['--host', 'unix:///var/run/docker.sock'])

    def test_formal_controls_require_disabled_swap_and_monitor(self):
        root, plan, sha = self.planned('controls')
        proc = self.root/'proc-fixture'
        (proc/'sys/kernel/random').mkdir(parents=True)
        (proc/'sys/kernel/random/boot_id').write_text('fixture-boot')
        (proc/'meminfo').write_text('SwapTotal:       0 kB\nSwapFree: 0 kB\n')
        (proc/'swaps').write_text('Filename\tType\tSize\tUsed\tPriority\n')
        good = ENV.formal_controls_snapshot(root, proc)
        self.assertTrue(good['passed'])
        (proc/'meminfo').write_text('SwapTotal: 4194304 kB\nSwapFree: 4194304 kB\n')
        self.assertFalse(ENV.formal_controls_snapshot(root, proc)['passed'])
        (proc/'meminfo').write_text('SwapTotal: 0 kB\n')
        (proc/'swaps').write_text('Filename Type Size Used Priority\n/swap.img file 1 0 -2\n')
        self.assertFalse(ENV.formal_controls_snapshot(root, proc)['passed'])
        (proc/'swaps').write_text('Filename Type Size Used Priority\n')
        config = root/'conf/rest-server.properties'
        config.write_text(config.read_text().replace('memory_monitor.threshold=1', 'memory_monitor.threshold=0.85'))
        self.assertFalse(ENV.formal_controls_snapshot(root, proc)['passed'])

    def test_calibration_freezes_formal_controls_and_blocks_before_docker(self):
        self.settings['purpose'] = 'calibration'
        root, plan, sha = self.planned('formal')
        self.assertEqual(plan['formalControlsContract'], 'hg-monitor-off-host-swap-off-v1')
        bad = {'passed': False, 'swapTotalBytes': 4096, 'activeSwapAreas': 1,
               'memoryMonitorDisabled': True, 'memoryMonitorThreshold': 1}
        stub = DockerStub(root, plan, sha)
        with patch.object(ENV, 'formal_controls_snapshot', return_value=bad), \
                patch.object(ENV, 'docker', side_effect=stub):
            result = ENV.serve(root)
        self.assertIn('formal-requires', result['error'])
        self.assertFalse(any(c[0] in ('create', 'start') for c in stub.calls))
        receipt = SERVICE.read_json(root/'formal-controls.json')
        self.assertFalse(receipt['allPassed'])
        self.assertEqual(receipt['maximumSwapTotalBytes'], 4096)

    def test_serve_is_single_dispatch_and_preserves_terminal_evidence(self):
        root, plan, sha = self.planned()
        stub = DockerStub(root, plan, sha)
        with patch.object(ENV, 'docker', side_effect=stub):
            result = ENV.serve(root)
            self.assertIsNone(result['error'])
            self.assertEqual(result['status']['state'], 'exited')
            self.assertTrue((root/'container-final.json').is_file())
            self.assertTrue((root/'supervisor-result.json').is_file())
            before = len(stub.calls)
            with self.assertRaises((SERVICE.EnvironmentError, FileExistsError)):
                ENV.serve(root)
            self.assertEqual(before, len(stub.calls))
            ENV.stop(root)
            ENV.stop(root)
            self.assertFalse(any(c[0] == 'rm' for c in stub.calls))

    def test_empty_docker_cidfile_is_pending_and_only_exact_owned_name_can_recover(self):
        root, plan, sha = self.planned()
        (root/'dispatch.json').write_text('{}')
        (root/'container.id').write_text('')
        stub = DockerStub(root, plan, sha)
        with patch.object(ENV, 'docker', side_effect=stub):
            self.assertEqual(ENV.status(root)['state'], 'dispatch-recorded')
            self.assertEqual(stub.calls[-1][-1], plan['containerName'])
            stub.info = inspected(plan, sha)
            self.assertEqual(ENV.status(root)['container']['id'], 'b'*64)
            stub.info['Config']['Labels'][ENV.LABEL+'.attempt'] = 'foreign'
            with self.assertRaisesRegex(SERVICE.EnvironmentError, 'owner-identity'):
                ENV.stop(root)
            self.assertFalse(any(c[0] == 'stop' for c in stub.calls))
            (root/'container.id').write_text('bad-partial-id')
            with self.assertRaisesRegex(SERVICE.EnvironmentError, 'invalid-container-id-file'):
                ENV.status(root)

    def test_empty_cidfile_stop_marks_cancellation_before_container_exists(self):
        root, plan, sha = self.planned()
        (root/'dispatch.json').write_text('{}')
        (root/'container.id').write_text('')
        missing = subprocess.CompletedProcess([], 1, b'[]', b'error: no such object')
        with patch.object(ENV, 'docker', return_value=missing):
            self.assertEqual(ENV.stop(root)['state'], 'dispatch-recorded')
            self.assertTrue(SERVICE.stop_requested(root, plan['attemptId'], sha))

    def test_unknown_start_outcome_stops_only_the_owned_id_and_reports_failure(self):
        root, plan, sha = self.planned()
        stub = DockerStub(root, plan, sha, fail_start=True)
        with patch.object(ENV, 'docker', side_effect=stub):
            result = ENV.serve(root)
        self.assertIn('injected-start', result['error'])
        stops = [c for c in stub.calls if c[0] == 'stop']
        self.assertEqual(len(stops), 1)
        self.assertEqual(stops[0][-1], 'b'*64)
        self.assertFalse(stub.info['State']['Running'])

    def test_forced_java_stop_cannot_be_reported_as_success_even_if_container_exit_zero(self):
        root, plan, sha = self.planned()
        stub = DockerStub(root, plan, sha)
        def force(plan, *args, **kwargs):
            result = stub(plan, *args, **kwargs)
            if args[0] == 'start':
                path = root/'work/service-result.json'
                terminal = SERVICE.read_json(path)
                terminal['outcomes'][-1]['termination'].update(killSent=True, exitCode=-9)
                SERVICE.write_json(path, terminal)
            return result
        with patch.object(ENV, 'docker', side_effect=force):
            result = ENV.serve(root)
        self.assertIn('verified-unforced-stop', result['error'])

    def test_stop_intent_is_published_before_inspection_and_validates_exact_identity(self):
        root, plan, sha = self.planned()
        (root/'dispatch.json').write_text('{}')
        stub = DockerStub(root, plan, sha)
        stub.info = inspected(plan, sha)
        def inspect_after_marker(plan, *args, **kwargs):
            self.assertTrue(SERVICE.stop_requested(root, plan['attemptId'], sha))
            return stub(plan, *args, **kwargs)
        with patch.object(ENV, 'docker', side_effect=inspect_after_marker):
            ENV.stop(root)
        self.assertTrue(SERVICE.stop_requested(root, plan['attemptId'], sha))
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'stop-marker-identity'):
            SERVICE.stop_requested(root, 'foreign', sha)

    def test_foreign_container_and_stop_before_dispatch_never_start_or_stop_unowned_work(self):
        root, plan, sha = self.planned()
        with patch.object(ENV, 'docker', side_effect=AssertionError('unstarted stop is offline')):
            ENV.stop(root)
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'already-stopped'):
            ENV.serve(root)
        (root/'dispatch.json').write_text('{}')
        stub = DockerStub(root, plan, sha)
        stub.info = inspected(plan, sha)
        stub.info['Config']['Labels'][ENV.LABEL+'.attempt'] = 'foreign'
        stub.info['State'].update(Status='running', Running=True)
        with patch.object(ENV, 'docker', side_effect=stub):
            with self.assertRaisesRegex(SERVICE.EnvironmentError, 'owner-identity'):
                ENV.stop(root)
        self.assertFalse(any(c[0] == 'stop' for c in stub.calls))

    def test_init_and_server_commands_use_explicit_correct_config_arguments(self):
        _, plan, _ = self.planned()
        commands = SERVICE.java_commands(plan)
        self.assertEqual(commands['init'][-2:], ['org.apache.hugegraph.cmd.InitStore',
                                               '/control/conf/rest-server.properties'])
        self.assertEqual(commands['server'][-4:], ['org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap', 'true',
                                                 '/control/conf/gremlin-server.yaml',
                                                 '/control/conf/rest-server.properties'])
        self.assertIn('@/opt/hugegraph/bin/jvm-module.options', commands['server'])
        self.assertIn('-Djava.security.properties=/control/conf/java-security.properties', commands['server'])
        self.assertNotIn('-Djava.security.manager=disallow', commands['server'])
        cp = commands['server'][commands['server'].index('-cp')+1]
        self.assertLess(cp.index('log4j-slf4j-impl'), cp.index('hugegraph-core.jar'))
        self.assertLess(cp.index('hugegraph-core.jar'), cp.index('other.jar'))
        self.assertIn('/ext/extra.jar', cp)
        init_cp = commands['init'][commands['init'].index('-cp')+1]
        self.assertNotIn('/ext/extra.jar', init_cp)
        self.assertLess(init_cp.index('hugegraph-core.jar'), init_cp.index('log4j-slf4j-impl'))

    def observation_settings(self):
        path = self.root/'jdk/lib/server/libjvm.so'
        path.parent.mkdir(parents=True)
        path.write_bytes(b'frozen-libjvm')
        (self.root/'jdk/release').write_text('JAVA_VERSION="17.0.17"\nJAVA_RUNTIME_VERSION="17.0.17+10"\n')
        self.settings['jvmObservation'] = observation_spec()

    def test_optional_observation_freezes_exact_spec_parser_plan_and_server_only_argv(self):
        self.observation_settings()
        root, plan, _ = self.planned()
        self.assertEqual(len(plan['controlFiles']), 13)
        spec, sha, _ = ENV.observation_component(root).load_plan(root/'jvm-observation')
        self.assertEqual(spec, self.settings['jvmObservation'])
        self.assertEqual(plan['jvmObservation'], {'planDirectory': 'jvm-observation', 'planSha256': sha,
                         'jdk': {'javaSha256': SERVICE.digest(self.root/'jdk/bin/java'),
                                 'libjvmSha256': SERVICE.digest(self.root/'jdk/lib/server/libjvm.so'),
                                 'version': '17.0.17+10'}})
        command = SERVICE.java_commands(plan)
        self.assertEqual([arg for arg in command['server'] if arg.startswith('-Xlog')], [OBSERVER.ARGUMENT])
        self.assertFalse(any(arg.startswith('-Xlog') for arg in command['init']))
        original = (root/'jvm-observation/spec.json').read_bytes()
        (root/'jvm-observation/spec.json').write_bytes(original+b' ')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'content-changed'):
            ENV.load_attempt(root, verify_sources=True)
        # Content verification must not prevent precise stop/status identity recovery.
        self.assertEqual(ENV.load_attempt(root)[1]['attemptId'], plan['attemptId'])

    def test_observation_rejects_different_budget_missing_libjvm_and_hidden_options(self):
        self.observation_settings()
        self.settings['jvmObservation']['maxRawBytes'] *= 2
        self.settings['jvmObservation']['parser']['maxOutputBytes'] += 65536
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'share-log-budget'):
            ENV.validate_settings(self.settings)
        self.settings['jvmObservation']['maxRawBytes'] //= 2
        file = self.root/'observer-settings.json'
        SERVICE.write_json(file, self.settings)
        library = self.root/'jdk/lib/server/libjvm.so'
        library.unlink()
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'member-required'):
            ENV.freeze(file, self.root/'no-libjvm')
        library.write_bytes(b'frozen-libjvm')
        for index, options in enumerate(('-Xlog:disable', '@other.options', '-agentlib:jdwp',
                                         '-Xlo\\\ng:disable')):
            (self.root/'distribution/bin/jvm-module.options').write_text(options)
            with self.subTest(options=options), self.assertRaisesRegex(SERVICE.EnvironmentError, 'unsafe-jvm-argfile'):
                ENV.freeze(file, self.root/('unsafe-'+str(index)))

    def test_observation_plan_presence_and_binding_cannot_be_forged(self):
        self.observation_settings()
        root, _, _ = self.planned()
        plan = SERVICE.read_json(root/'plan.json')
        plan['jvmObservation']['jdk']['libjvmSha256'] = 'f'*64
        SERVICE.write_json(root/'plan.json', plan)
        SERVICE.write_json(root/'plan.lock.json', {'schemaVersion': 1, 'attemptId': plan['attemptId'],
                                                'planSha256': SERVICE.digest(root/'plan.json')})
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'binding-mismatch'):
            ENV.load_attempt(root, verify_sources=True)
        del plan['jvmObservation']
        SERVICE.write_json(root/'plan.json', plan)
        SERVICE.write_json(root/'plan.lock.json', {'schemaVersion': 1, 'attemptId': plan['attemptId'],
                                                'planSha256': SERVICE.digest(root/'plan.json')})
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'presence-mismatch'):
            ENV.load_attempt(root)


class JvmRuntimeEvidence(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hgbench-jvm-runtime-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.proc, self.java = self.root/'proc', self.root/'jdk'
        self.pid, self.ticks = 123, '41'
        self.process = self.proc/str(self.pid)
        self.process.mkdir(parents=True)
        for relative, value in [('bin/java', b'java-binary'), ('lib/server/libjvm.so', b'libjvm-binary')]:
            path = self.java/relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
        (self.process/'exe').symlink_to(self.java/'bin/java')
        self.argv = ['/opt/jdk/bin/java', OBSERVER.ARGUMENT, '-cp', 'frozen', 'Main']
        (self.process/'cmdline').write_bytes(b'\0'.join(a.encode() for a in self.argv)+b'\0')
        (self.process/'stat').write_text('123 (java process) S '+'0 '*18+self.ticks+' 0\n')
        self.boot = '12345678-1234-1234-1234-123456789abc'
        boot = self.proc/'sys/kernel/random/boot_id'
        boot.parent.mkdir(parents=True)
        boot.write_text(self.boot+'\n')
        for root in (self.proc/'self', self.process):
            (root/'ns').mkdir(parents=True)
            (root/'ns/time').symlink_to('time:[4026531834]')
            (root/'ns/time_for_children').symlink_to('time:[4026531834]')
            (root/'timens_offsets').write_text('monotonic 0 0\nboottime 0 0\n')
        self.observation = {'planDirectory': 'jvm-observation', 'planSha256': 'a'*64,
                            'jdk': {'javaSha256': SERVICE.digest(self.java/'bin/java'),
                                    'libjvmSha256': SERVICE.digest(self.java/'lib/server/libjvm.so'),
                                    'version': '17.0.17+10'}}
        self.library = (self.java/'lib/server/libjvm.so').stat()
        self.maps = ('1000-2000 r-xp 0000 '+f'{os.major(self.library.st_dev):02x}:'+
                     f'{os.minor(self.library.st_dev):02x} '+str(self.library.st_ino)+' '+
                     str(self.java/'lib/server/libjvm.so')+'\n')
        (self.process/'maps').write_text(self.maps)

    def inspect(self):
        with patch.object(SERVICE.sys, 'platform', 'linux'):
            return SERVICE.inspect_jvm_process(self.pid, self.ticks, self.argv, self.observation, lambda: None,
                                               proc_root=self.proc, java_home=self.java)

    def test_launch_proves_actual_command_executable_loaded_library_and_raw_identity(self):
        launch, runtime = self.inspect()
        self.assertEqual(launch['argv'], self.argv)
        self.assertEqual(launch['process'], {'pid': self.pid, 'startTimeTicks': self.ticks,
                                            'hostBootId': self.boot})
        self.assertEqual(launch['jdk'], self.observation['jdk'])
        process = runtime['process']
        self.assertEqual(base64.b64decode(process['mapsBase64']), self.maps.encode())
        self.assertEqual(base64.b64decode(process['statBeforeBase64']), (self.process/'stat').read_bytes())
        self.assertEqual(process['statBeforeBase64'], process['statAfterBase64'])
        self.assertEqual(base64.b64decode(process['cmdlineBase64']), (self.process/'cmdline').read_bytes())
        self.assertEqual(runtime['clock']['capture']['offsets']['value'], 'monotonic 0 0\nboottime 0 0\n')
        self.assertEqual(runtime['clock']['phaseMapping'], 'unverified')
        self.assertEqual(process['libjvm']['inode'], str(self.library.st_ino))
        identity = runtime['captureIdentity']
        self.assertEqual(identity['kind'], 'http-jvm-initial-clock-identity')
        self.assertEqual(identity['sourceOrigin'], 'fixture')
        self.assertIsNone(identity['snapshot']['capture'])
        self.assertFalse(identity['verified'])

    def test_clock_brackets_enclose_capture_and_all_namespace_reads_before_runtime_finish(self):
        reads = []
        original = SERVICE.clock_namespace
        def namespace(root):
            value = original(root)
            reads.append(time.monotonic_ns())
            return value
        with patch.object(SERVICE, 'clock_namespace', side_effect=namespace):
            _, runtime = self.inspect()
        clock, identity = runtime['clock'], runtime['captureIdentity']
        self.assertEqual(len(reads), 2)
        self.assertLessEqual(int(runtime['observedBeforeMonotonicNs']), int(clock['observedBeforeMonotonicNs']))
        self.assertLessEqual(int(clock['observedBeforeMonotonicNs']),
                             int(identity['snapshot']['startedMonotonicNs']))
        self.assertLessEqual(int(identity['snapshot']['finishedMonotonicNs']), reads[0])
        self.assertLessEqual(reads[-1], int(clock['observedAfterMonotonicNs']))
        self.assertLessEqual(int(clock['observedAfterMonotonicNs']), int(runtime['observedAfterMonotonicNs']))

    def test_unknown_initial_capture_keeps_independent_process_proof_and_reason(self):
        snapshot = SERVICE.CLOCK.clock_snapshot(self.pid, True, self.proc)
        snapshot['unavailable']['source'] = 'fixture-has-no-production-clock-identity'
        with patch.object(SERVICE.CLOCK, 'clock_snapshot', return_value=snapshot):
            launch, runtime = self.inspect()
        self.assertEqual(launch['process']['startTimeTicks'], self.ticks)
        self.assertEqual(runtime['process']['libjvm']['inode'], str(self.library.st_ino))
        self.assertEqual(runtime['captureIdentity']['snapshot']['unavailable']['source'],
                         'fixture-has-no-production-clock-identity')

    def test_mapping_not_loaded_yet_waits_but_foreign_path_inode_and_deleted_map_fail(self):
        (self.process/'maps').write_text('')
        self.assertIsNone(self.inspect())
        changes = [self.maps.replace(str(self.library.st_ino), str(self.library.st_ino+1)),
                   self.maps.replace(str(self.java), '/foreign-jdk'), self.maps.rstrip()+' (deleted)\n',
                   self.maps.replace('r-xp', 'r--p')]
        for value in changes:
            (self.process/'maps').write_text(value)
            with self.subTest(value=value), self.assertRaises(SERVICE.EnvironmentError):
                self.inspect()

    def test_actual_argv_pid_start_executable_and_binary_hash_are_required(self):
        original = (self.process/'cmdline').read_bytes()
        (self.process/'cmdline').write_bytes(original.replace(b'frozen', b'changed'))
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'argv-mismatch'):
            self.inspect()
        (self.process/'cmdline').write_bytes(original)
        self.ticks = '42'
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'process-replaced'):
            self.inspect()
        self.ticks = '41'
        other = self.java/'bin/other'
        other.write_bytes(b'java-binary')
        (self.process/'exe').unlink()
        (self.process/'exe').symlink_to(other)
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'executable-mismatch'):
            self.inspect()
        (self.process/'exe').unlink()
        (self.process/'exe').symlink_to(self.java/'bin/java')
        (self.java/'lib/server/libjvm.so').write_bytes(b'changed-binary')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'content-changed'):
            self.inspect()

    def test_clock_missing_is_explicit_unavailable_and_source_reads_are_bounded(self):
        (self.proc/'self/ns/time').unlink()
        (self.process/'timens_offsets').unlink()
        _, runtime = self.inspect()
        self.assertFalse(runtime['clock']['capture']['namespace']['available'])
        self.assertIsNone(runtime['clock']['capture']['namespace']['value'])
        self.assertFalse(runtime['clock']['jvm']['offsets']['available'])
        (self.process/'cmdline').write_bytes(b'x'*131073)
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'record-too-large'):
            self.inspect()

    def test_capture_receipt_binds_closed_prefix_and_does_not_invent_completeness(self):
        work = self.root/'work'
        work.mkdir()
        launch, _ = self.inspect()
        SERVICE.write_json(work/'jvm-launch.json', launch)
        fields = {'startedMonotonicNs': '100', 'finishedMonotonicNs': '200', 'eof': True, 'capHit': True,
                  'forcedKill': True, 'error': 'EIO', 'readBytes': 20, 'retainedBytes': 10,
                  'discardedBytes': 10, 'sha256': hashlib.sha256(b'raw-prefix').hexdigest()}
        receipt = SERVICE.jvm_capture_receipt(work, self.observation, fields, {'cleanupComplete': False})
        OBSERVER.validate_evidence(launch, receipt, self.observation['planSha256'],
                                  SERVICE.digest(work/'jvm-launch.json'))
        self.assertEqual(receipt['raw']['bytes'], 10)
        self.assertEqual(receipt['error'], 'EIO')
        self.assertTrue(receipt['capHit'])
        self.assertFalse(receipt['cleanupComplete'])
        self.assertNotIn('complete', receipt)
        self.assertEqual(receipt, SERVICE.read_json(work/'jvm-capture.json'))


class Watchdog(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hgbench-watchdog-test-')
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.budget = {'maxLogicalBytes': 1024**2, 'maxAllocatedBytes': 2*1024**2, 'minFreeBytes': 0,
                       'maxLogBytes': 4096, 'startupSeconds': 0.15, 'shutdownSeconds': 0.1,
                       'pollSeconds': 0.02}
        self.state = {'attemptId': 'unit', 'planSha256': 'a'*64, 'mode': 'forward',
                      'startedMonotonic': time.monotonic(), 'logBytes': 0}

    def run_child(self, program, seconds=2, cancelled=None, readiness=None):
        return SERVICE.supervise([sys.executable, '-u', '-c', program], self.work, self.budget,
                                 time.monotonic()+seconds, 'server', cancelled or [False], self.state, readiness)

    def test_watchdog_reaps_clean_exit_and_limits_logs_without_host_controller(self):
        result = self.run_child('print("done")')
        self.assertEqual(result['reason'], 'process-exit')
        self.assertEqual(result['exitCodeBeforeShutdown'], 0)
        self.assertEqual((self.work/'server.log').read_text(), 'done\n')

    def test_log_limit_terminates_writer_and_preserves_bounded_partial_evidence(self):
        result = self.run_child('import time\nwhile True: print("x"*10000, flush=True)')
        self.assertEqual(result['reason'], 'log-byte-budget')
        self.assertEqual((self.work/'server.log').stat().st_size, 4096)
        self.assertTrue(result['capture']['capHit'])
        self.assertFalse(result['capture']['complete'])

    def assert_capture(self, result, name='server'):
        capture = result['capture']
        raw = (self.work/(name+'.log')).read_bytes()
        self.assertEqual(capture['retainedBytes'], len(raw))
        self.assertEqual(capture['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(capture['readBytes'], capture['retainedBytes'] + capture['discardedBytes'])
        self.assertGreaterEqual(int(capture['finishedMonotonicNs']), int(capture['startedMonotonicNs']))
        self.assertEqual(capture, json.loads((self.work/(name+'-capture.json')).read_text()))
        return capture

    def test_term_hook_larger_than_pipe_is_drained_and_sealed_without_kill(self):
        self.budget.update(maxLogBytes=700000, shutdownSeconds=1)
        program = ('import os,signal,time,sys\n'
                   'def stop(*args):\n'
                   ' data=b"x"*524288+b"TAIL-END\\n"\n'
                   ' while data:\n'
                   '  n=os.write(1,data); data=data[n:]\n'
                   ' sys.exit(0)\n'
                   'signal.signal(signal.SIGTERM,stop)\n'
                   'open("hook-ready","w").close()\n'
                   'while True: time.sleep(.01)\n')
        result = SERVICE.supervise([sys.executable, '-u', '-c', program], self.work, self.budget,
                                   time.monotonic()+3, 'server', [False], self.state,
                                   stop_check=lambda: (self.work/'hook-ready').exists())
        self.assertTrue(SERVICE.unforced_stop(result))
        self.assertTrue(self.assert_capture(result)['complete'])
        self.assertTrue((self.work/'server.log').read_bytes().endswith(b'TAIL-END\n'))

    def test_whole_arm_alarm_during_term_drain_does_not_interrupt_reaping(self):
        previous = signal.signal(signal.SIGALRM, lambda _sig, _frame: SERVICE.expire_budget(self.state))
        self.budget['shutdownSeconds'] = 1
        armed = []
        def stop():
            if not (self.work/'hook-ready').exists():
                return False
            if not armed:
                armed.append(True)
                signal.setitimer(signal.ITIMER_REAL, 0.05)
            return True
        program = ('import signal,time,sys\n'
                   'def stop(*args):\n time.sleep(.15);print("FINAL-TERM");sys.exit(0)\n'
                   'signal.signal(signal.SIGTERM,stop)\n'
                   'open("hook-ready","w").close()\n'
                   'while True: time.sleep(.01)\n')
        try:
            result = SERVICE.supervise([sys.executable, '-u', '-c', program], self.work, self.budget,
                                       time.monotonic()+3, 'server', [False], self.state, stop_check=stop)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        self.assertTrue(self.state['deadlineExpiredDuringCleanup'])
        self.assertTrue(SERVICE.unforced_stop(result))
        self.assertTrue(self.assert_capture(result)['complete'])
        self.assertEqual((self.work/'server.log').read_bytes(), b'FINAL-TERM\n')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'wall-clock-budget'):
            SERVICE.expire_budget(self.state)

    def test_exact_cap_and_one_extra_byte_share_budget_across_phases(self):
        for phase, count in [('init', 12), ('server', 4084), ('extra', 1)]:
            result = SERVICE.supervise([sys.executable, '-u', '-c', f'import os;os.write(1,b"x"*{count})'],
                                       self.work, self.budget, time.monotonic()+2, phase, [False], self.state)
            capture = self.assert_capture(result, phase)
            self.assertEqual(capture['capHit'], phase == 'extra')
            self.assertEqual(capture['complete'], phase != 'extra')
        self.assertEqual(self.state['logBytes'], 4096)
        self.assertEqual((self.work/'extra.log').stat().st_size, 0)

    def test_observation_readiness_waits_for_identity_and_capture_binds_real_pipe(self):
        observation = {'planSha256': 'a'*64, 'jdk': {'javaSha256': 'b'*64, 'libjvmSha256': 'c'*64,
                                                   'version': '17.0.17+10'}}
        command = [sys.executable, '-u', '-c', 'import time;print("server",flush=True);time.sleep(20)',
                   OBSERVER.ARGUMENT]
        def inspect(pid, start, argv, setting, check):
            self.assertFalse((self.work/'ready.json').exists())
            self.assertEqual(setting, observation)
            check()
            launch = {'schemaVersion': 1, 'kind': 'http-jvm-observation-launch', 'planSha256': 'a'*64,
                      'argv': argv, 'process': {'pid': pid, 'startTimeTicks': start or '41',
                                               'hostBootId': '12345678-1234-1234-1234-123456789abc'},
                      'jdk': observation['jdk']}
            return launch, {'schemaVersion': 1, 'kind': 'unit-runtime-receipt'}
        with patch.object(SERVICE, 'inspect_jvm_process', side_effect=inspect):
            result = SERVICE.supervise(command, self.work, self.budget, time.monotonic()+2, 'server', [False],
                                       self.state, readiness=lambda _deadline: True,
                                       stop_check=lambda: (self.work/'ready.json').exists(),
                                       jvm_observation=observation)
        self.assertTrue(result['ready'])
        self.assertTrue(result['jvmObservationRecorded'])
        self.assertTrue(SERVICE.unforced_stop(result))
        launch = SERVICE.read_json(self.work/'jvm-launch.json')
        receipt = SERVICE.read_json(self.work/'jvm-capture.json')
        OBSERVER.validate_evidence(launch, receipt, observation['planSha256'],
                                  SERVICE.digest(self.work/'jvm-launch.json'))
        self.assertEqual(receipt['raw']['sha256'], SERVICE.digest(self.work/'server.log'))
        self.assertEqual(receipt['raw']['bytes'], (self.work/'server.log').stat().st_size)
        self.assertEqual(receipt['eof'], result['capture']['eof'])
        self.assertEqual(receipt['cleanupComplete'], result['termination']['cleanupComplete'])
        self.assertEqual(SERVICE.read_json(self.work/'jvm-runtime.json')['launchSha256'],
                         SERVICE.digest(self.work/'jvm-launch.json'))

    def test_missing_jvm_mapping_never_publishes_ready_or_successful_launch_receipt(self):
        with patch.object(SERVICE, 'inspect_jvm_process', return_value=None):
            result = SERVICE.supervise([sys.executable, '-c', 'import time;time.sleep(20)'], self.work,
                                       self.budget, time.monotonic()+2, 'server', [False], self.state,
                                       readiness=lambda _deadline: True, jvm_observation={'planSha256': 'a'*64})
        self.assertEqual(result['reason'], 'startup-timeout')
        self.assertFalse(result['ready'])
        self.assertFalse(result['jvmObservationRecorded'])
        self.assertFalse((self.work/'ready.json').exists())
        self.assertFalse((self.work/'jvm-launch.json').exists())
        self.assertFalse((self.work/'jvm-capture.json').exists())

    def test_invalid_jvm_identity_stops_child_and_preserves_failure_capture(self):
        with patch.object(SERVICE, 'inspect_jvm_process',
                          side_effect=SERVICE.EnvironmentError('injected-jvm-runtime-argv-mismatch')):
            result = SERVICE.supervise([sys.executable, '-c', 'import time;time.sleep(20)'], self.work,
                                       self.budget, time.monotonic()+2, 'server', [False], self.state,
                                       readiness=lambda _deadline: True, jvm_observation={'planSha256': 'a'*64})
        self.assertEqual(result['reason'], 'watchdog-error')
        self.assertIn('argv-mismatch', result['error'])
        self.assertFalse((self.work/'ready.json').exists())
        self.assertTrue(result['termination']['cleanupComplete'])
        self.assertTrue(self.assert_capture(result)['eof'])

    def test_natural_exit_final_output_is_drained_before_capture_seal(self):
        self.budget['maxLogBytes'] = 400000
        result = self.run_child('import os\ndata=b"x"*262144+b"NATURAL-END"\n'
                                'while data:\n n=os.write(1,data); data=data[n:]')
        self.assertTrue(self.assert_capture(result)['complete'])
        self.assertTrue((self.work/'server.log').read_bytes().endswith(b'NATURAL-END'))

    def test_write_failure_keeps_draining_and_marks_capture_partial(self):
        class BrokenLog:
            def write(self, _raw):
                raise OSError('injected-disk-write-failure')
        read_fd, write_fd = os.pipe()
        with os.fdopen(read_fd, 'rb', buffering=0) as read:
            selector = selectors.DefaultSelector()
            self.addCleanup(selector.close)
            selector.register(read, selectors.EVENT_READ)
            capture = SERVICE.PipeCapture(selector, BrokenLog(), self.state, 4096)
            os.write(write_fd, b'abc')
            os.close(write_fd)
            capture.drain(0)
            capture.drain(0)
            result = capture.finish({'killSent': False, 'cleanupComplete': True})
            self.assertEqual(result['readBytes'], 3)
            self.assertEqual(result['discardedBytes'], 3)
            self.assertEqual(result['retainedBytes'], 0)
            self.assertTrue(result['eof'])
            self.assertFalse(result['complete'])
            self.assertIn('injected-disk', result['error'])

    def test_short_write_hashes_only_the_confirmed_prefix(self):
        class ShortLog:
            def write(self, _raw):
                return 2
        read_fd, write_fd = os.pipe()
        with os.fdopen(read_fd, 'rb', buffering=0) as read:
            selector = selectors.DefaultSelector()
            self.addCleanup(selector.close)
            selector.register(read, selectors.EVENT_READ)
            capture = SERVICE.PipeCapture(selector, ShortLog(), self.state, 4096)
            os.write(write_fd, b'abcd')
            os.close(write_fd)
            capture.drain(0)
            capture.drain(0)
            result = capture.finish({'killSent': False, 'cleanupComplete': True})
            self.assertEqual(result['retainedBytes'], 2)
            self.assertEqual(result['discardedBytes'], 2)
            self.assertEqual(result['sha256'], hashlib.sha256(b'ab').hexdigest())
            self.assertFalse(result['complete'])

    def test_ttl_kills_process_group_including_child_holding_stdout(self):
        ready = self.work / 'descendant-ready'
        program = ('import subprocess,sys\np=subprocess.Popen([sys.executable,"-c",'
                   '"import time;time.sleep(30)"])\nprint(p.pid,flush=True)\n'
                   'open("descendant-ready","w").close()')
        # Expire only after the real descendant exists and its PID has been
        # flushed. A 250ms startup race could exercise expiry before spawning it.
        clock = SimpleNamespace(monotonic=lambda: time.monotonic() + (10 if ready.exists() else 0),
                                monotonic_ns=time.monotonic_ns)
        with patch.object(SERVICE, 'time', clock):
            result = self.run_child(program, seconds=5)
        self.assertEqual(result['reason'], 'wall-clock-budget')
        self.assertTrue(ready.exists(), 'descendant was not spawned before expiry')
        pid = int((self.work/'server.log').read_text().strip())
        deadline = time.monotonic() + 2
        observed = 'not-observed'
        while True:
            if Path('/proc').exists():
                try:
                    raw = (Path('/proc')/str(pid)/'stat').read_text()
                    observed = raw.rsplit(')', 1)[1].split()[0]
                except FileNotFoundError:
                    observed = 'gone'
            else:
                query = subprocess.run(['/bin/ps', '-o', 'stat=', '-p', str(pid)],
                                       capture_output=True, text=True, timeout=1)
                observed = query.stdout.strip() if query.returncode == 0 else 'gone'
            # Signal delivery precedes actual death; a killed orphan may then await PID 1's zombie reap.
            if observed == 'gone' or observed.startswith(('Z', 'X')):
                break
            if time.monotonic() >= deadline:
                self.fail(f'Owned descendant {pid} remained live after group shutdown: {observed}')
            time.sleep(0.01)

    def test_disk_and_startup_guards_terminate_children(self):
        result = self.run_child('import time\nopen("excess","wb").write(b"x"*(2*1024*1024))\ntime.sleep(20)')
        self.assertEqual(result['reason'], 'directory-logical-budget')

    def test_readiness_timeout_and_explicit_cancel(self):
        result = self.run_child('import time;time.sleep(20)', readiness=lambda _deadline: False)
        self.assertEqual(result['reason'], 'startup-timeout')

    def test_cancel_is_independent_of_docker_or_host_ssh(self):
        cancelled = [False]
        timer = threading.Timer(0.1, lambda: cancelled.__setitem__(0, True))
        timer.start()
        try:
            result = self.run_child('import time;time.sleep(20)', cancelled=cancelled)
        finally:
            timer.join()
        self.assertEqual(result['reason'], 'stop-requested')

    def test_runtime_symlinks_cannot_hide_disk_usage(self):
        (self.work/'escape').symlink_to('/tmp')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'symlink'):
            SERVICE.directory_usage(self.work)

    def test_directory_scan_retries_disappearing_names_and_marks_instability(self):
        moving = self.work/'moving'
        moving.write_bytes(b'x')
        original = Path.lstat
        removed = []
        def lstat(path, *args, **kwargs):
            if path == moving and not removed:
                moving.unlink()
                removed.append(True)
            return original(path, *args, **kwargs)
        with patch.object(Path, 'lstat', lstat):
            observed = SERVICE.directory_usage(self.work)
        self.assertTrue(observed['unstable'])
        self.assertEqual(observed['resamples'], 1)
        with patch.object(SERVICE, '_directory_usage', side_effect=FileNotFoundError()):
            with self.assertRaisesRegex(SERVICE.EnvironmentError, 'remained-unstable'):
                SERVICE.directory_usage(self.work)
        with patch.object(SERVICE, '_directory_usage', side_effect=PermissionError()):
            with self.assertRaises(PermissionError):
                SERVICE.directory_usage(self.work)

    def test_stop_marker_blocks_java_launch_after_create_start_race(self):
        with patch.object(SERVICE.subprocess, 'Popen', side_effect=AssertionError('must not start Java')):
            result = SERVICE.supervise([sys.executable, '-c', 'print(1)'], self.work, self.budget,
                                       time.monotonic()+2, 'server', [False], self.state,
                                       stop_check=lambda: True)
        self.assertEqual(result['reason'], 'watchdog-error')
        self.assertIn('before-java-launch', result['error'])
        self.assertFalse((self.work/'server.log').exists())

    def test_term_ignoring_child_records_forced_kill_and_cannot_pass_unforced_stop(self):
        program = ('import signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\n'
                   'open("term-ignore-ready","w").close()\ntime.sleep(20)')
        result = SERVICE.supervise([sys.executable, '-u', '-c', program], self.work, self.budget,
                                   time.monotonic()+3, 'server', [False], self.state,
                                   stop_check=lambda: (self.work/'term-ignore-ready').exists())
        self.assertTrue(result['termination']['termSent'])
        self.assertTrue(result['termination']['killSent'])
        self.assertEqual(result['termination']['exitCode'], -signal.SIGKILL)
        self.assertFalse(SERVICE.unforced_stop(result))
        capture = self.assert_capture(result)
        self.assertTrue(capture['forcedKill'])
        self.assertFalse(capture['complete'])

    def test_documented_hotspot_term_exit_codes_require_complete_unforced_cleanup(self):
        for code in (0, 143, -signal.SIGTERM):
            outcome = {'reason': 'stop-requested', 'termination': {'cleanupComplete': True, 'termSent': True,
                                                                 'killSent': False, 'exitCode': code}}
            self.assertTrue(SERVICE.unforced_stop(outcome))
            outcome['termination']['cleanupComplete'] = False
            self.assertFalse(SERVICE.unforced_stop(outcome))

    def test_late_health_callback_cannot_publish_ready(self):
        def delayed(_deadline):
            time.sleep(0.2)
            return True
        result = self.run_child('import time;time.sleep(20)', readiness=delayed)
        self.assertEqual(result['reason'], 'startup-timeout')
        self.assertFalse((self.work/'ready.json').exists())

    def test_each_hash_chunk_and_directory_checks_deadline_and_cancel(self):
        path = self.work/'three-megabytes'
        path.write_bytes(b'x'*(3*1024**2))
        calls = []
        def checked():
            calls.append(True)
            if len(calls) == 4:
                raise SERVICE.EnvironmentError('startup-budget-exhausted')
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'startup-budget-exhausted'):
            SERVICE.digest(path, checked)
        self.assertEqual(len(calls), 4)
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'stop-requested'):
            SERVICE.inventory(self.work, check=lambda: SERVICE.check_deadline(time.monotonic()+5, [True]))
        with self.assertRaisesRegex(SERVICE.EnvironmentError, 'startup-budget-exhausted'):
            SERVICE.inventory(self.work, check=lambda: SERVICE.check_deadline(time.monotonic()-1, [False]))

    def test_health_absolute_deadline_interrupts_byte_trickle(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        class Trickle(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length', 0)))
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                try:
                    for value in b'{"status":{"code":200},"result":{"data":[2]}}':
                        self.wfile.write(bytes([value]))
                        self.wfile.flush()
                        time.sleep(0.025)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            def log_message(self, *_args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Trickle)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        original = SERVICE.http.client.HTTPConnection
        try:
            with patch.object(SERVICE.http.client, 'HTTPConnection',
                              side_effect=lambda _host, _port, timeout: original('127.0.0.1', server.server_port,
                                                                                timeout=timeout)):
                started = time.monotonic()
                self.assertFalse(SERVICE.healthy(started+0.15))
                self.assertLess(time.monotonic()-started, 0.5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == '__main__':
    unittest.main()
