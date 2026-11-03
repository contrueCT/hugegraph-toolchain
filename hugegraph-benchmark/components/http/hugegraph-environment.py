#!/usr/bin/env python3
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

"""Plan/serve/status/stop one controlled Docker HugeGraph HTTP experiment arm.

plan is offline and creates a new immutable attempt. serve is a foreground
supervisor suitable for hgbench task or systemd; disconnects never authorize a
second serve. status and stop recover the exact labeled container. No shell,
image pulls, host ports, graph data loading, mode inference or performance claim.
"""
import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location('hgbench_http_service', Path(__file__).with_name('hugegraph-service.py'))
SERVICE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SERVICE)
require, read_json, write_json = SERVICE.require, SERVICE.read_json, SERVICE.write_json
digest, inventory, verify_inventory = SERVICE.digest, SERVICE.inventory, SERVICE.verify_inventory
LABEL = 'org.apache.hugegraph.hgbench.http'


def exact(value, keys, label):
    require(isinstance(value, dict) and set(value) == set(keys), 'invalid-' + label + '-fields')


def integer(value, low, high, label):
    require(type(value) is int and low <= value <= high, 'invalid-' + label)


def observation_component(root=None):
    path = (Path(root) if root else Path(__file__).parent) / 'jvm-observation.py'
    spec = importlib.util.spec_from_file_location('hgbench_jvm_observation', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_settings(settings):
    fields = ('schemaVersion', 'kind', 'purpose', 'distribution', 'javaHome', 'image', 'python',
              'dockerSocket', 'mode', 'budget', 'jvm', 'server')
    exact(settings, fields + (('jvmObservation',) if 'jvmObservation' in settings else ()), 'settings')
    require(settings['schemaVersion'] == 1 and settings['kind'] == 'hugegraph-http-environment-settings',
            'invalid-settings-identity')
    require(settings['purpose'] in ('functional', 'calibration'), 'invalid-purpose')
    require(settings['mode'] in ('forward', 'embedded'), 'invalid-mode')
    require(re.fullmatch(r'sha256:[a-f0-9]{64}', settings['image']) is not None, 'immutable-image-id-required')
    for key in ('distribution', 'javaHome', 'dockerSocket', 'python'):
        value = settings[key]
        require(isinstance(value, str) and value.startswith('/') and not any(c in value for c in '\n\r\0,'),
                'invalid-' + key + '-path')
    budget = settings['budget']
    exact(budget, ('cpus', 'memoryBytes', 'pids', 'wallSeconds', 'startupSeconds', 'shutdownSeconds',
                   'maxAllocatedBytes', 'maxLogicalBytes', 'maxLogBytes', 'minFreeBytes', 'tmpfsBytes',
                   'pollSeconds'), 'budget')
    require(type(budget['cpus']) in (int, float) and math.isfinite(budget['cpus']) and 0.1 <= budget['cpus'] <= 64,
            'invalid-cpus')
    require(type(budget['pollSeconds']) in (int, float) and 0.1 <= budget['pollSeconds'] <= 5,
            'invalid-poll-seconds')
    bounds = {'memoryBytes': (512 * 1024**2, 128 * 1024**3), 'pids': (64, 4096), 'wallSeconds': (5, 86400),
              'startupSeconds': (1, 3600), 'shutdownSeconds': (1, 30),
              'maxAllocatedBytes': (1024**2, 64 * 1024**3), 'maxLogicalBytes': (1024**2, 128 * 1024**3),
              'maxLogBytes': (4096, 256 * 1024**2), 'minFreeBytes': (0, 1024**5),
              'tmpfsBytes': (4 * 1024**2, 1024**3)}
    for key, (low, high) in bounds.items():
        integer(budget[key], low, high, key)
    require(budget['startupSeconds'] <= budget['wallSeconds'] and
            budget['maxLogBytes'] <= budget['maxLogicalBytes'] and budget['tmpfsBytes'] < budget['memoryBytes'],
            'inconsistent-budgets')
    jvm = settings['jvm']
    exact(jvm, ('heapMinBytes', 'heapMaxBytes', 'maxDirectMemoryBytes', 'activeProcessorCount'), 'jvm')
    for key in ('heapMinBytes', 'heapMaxBytes', 'maxDirectMemoryBytes'):
        integer(jvm[key], 16 * 1024**2, budget['memoryBytes'], key)
    integer(jvm['activeProcessorCount'], 1, math.ceil(budget['cpus']), 'active-processor-count')
    require(jvm['heapMinBytes'] <= jvm['heapMaxBytes'] and
            jvm['heapMaxBytes'] + jvm['maxDirectMemoryBytes'] + 128 * 1024**2 <= budget['memoryBytes'],
            'insufficient-jvm-memory-headroom')
    server = settings['server']
    exact(server, ('gremlinPool', 'bossThreads', 'workerThreads', 'maxWorkQueueSize', 'httpWorkerThreads',
                   'maxRoute', 'timeoutSeconds', 'idleTimeoutSeconds', 'connectionMaxRequests',
                   'maxContentLengthBytes', 'resultIterationBatchSize'), 'server')
    for key in ('gremlinPool', 'bossThreads', 'workerThreads', 'maxRoute'):
        integer(server[key], 1, 256, key)
    integer(server['httpWorkerThreads'], 2, 256, 'http-worker-threads')
    integer(server['maxWorkQueueSize'], 1, 65536, 'work-queue-size')
    for key in ('timeoutSeconds', 'idleTimeoutSeconds'):
        integer(server[key], 1, 3600, key)
    integer(server['connectionMaxRequests'], 1, 1000000, 'connection-max-requests')
    integer(server['maxContentLengthBytes'], 4096, 4 * 1024**2, 'max-content-length')
    integer(server['resultIterationBatchSize'], 1, 4096, 'result-iteration-batch-size')
    if 'jvmObservation' in settings:
        observation_component().validate_spec(settings['jvmObservation'])
        require(settings['jvmObservation']['maxRawBytes'] == budget['maxLogBytes'],
                'jvm-observation-must-share-log-budget')


def observation_jdk(settings, sources):
    """The optional observer requires exact regular HotSpot binary members."""
    root = Path(settings['javaHome'])
    files = {item['path']: item for item in sources['javaHome']['files']}
    for name in ('bin/java', 'lib/server/libjvm.so'):
        require(name in files and files[name]['type'] == 'file', 'jvm-observation-jdk-member-required')
    release = root / 'release'
    require(release.stat().st_size <= 65536, 'jdk-release-too-large')
    raw = release.read_text()
    values = re.findall(r'^(?:JAVA_RUNTIME_VERSION|JAVA_VERSION)="([^"\n]+)"$', raw, re.M)
    require(values and all(re.fullmatch(r'17\.[0-9.]+(?:\+[A-Za-z0-9._-]+)?', v) for v in values),
            'jvm-observation-jdk17-version-required')
    runtime = re.findall(r'^JAVA_RUNTIME_VERSION="([^"\n]+)"$', raw, re.M)
    require(len(runtime) <= 1, 'duplicate-jdk-runtime-version')
    options = (Path(settings['distribution']) / 'bin/jvm-module.options').read_bytes()
    # The shipped file contains module opens/exports only. Reject every possible
    # hidden logging/argfile/agent route rather than approximate Java's tokenizer.
    require(len(options) <= 65536 and all(byte < 128 for byte in options) and
            not any(token in options for token in (b'Xlog', b'Xrun', b'agent', b'@', b'\\')),
            'jvm-observation-unsafe-jvm-argfile')
    return {'javaSha256': files['bin/java']['sha256'],
            'libjvmSha256': files['lib/server/libjvm.so']['sha256'],
            'version': runtime[0] if runtime else values[0]}


def configurations(settings, template):
    server = settings['server']
    changes = {'host': '127.0.0.1', 'port': 18182, 'gremlinPool': server['gremlinPool'],
               'threadPoolBoss': server['bossThreads'], 'threadPoolWorker': server['workerThreads'],
               'maxWorkQueueSize': server['maxWorkQueueSize'], 'evaluationTimeout': server['timeoutSeconds'] * 1000,
               'maxContentLength': server['maxContentLengthBytes'],
               'resultIterationBatchSize': server['resultIterationBatchSize']}
    for key in changes:
        require(len(re.findall(r'^' + re.escape(key) + r'\s*:', template, re.M)) <= 1, 'duplicate-yaml-key')
        template = re.sub(r'^' + re.escape(key) + r'\s*:.*(?:\n|$)', '', template, flags=re.M)
    gremlin = ''.join(f'{key}: {value}\n' for key, value in changes.items()) + template
    properties = {'graphs': '/control/conf/graphs', 'restserver.url': 'http://127.0.0.1:18080',
                  'gremlinserver.url': 'http://127.0.0.1:18182',
                  'gremlinserver.rest_execution_mode': settings['mode'],
                  'restserver.max_worker_threads': server['httpWorkerThreads'],
                  'gremlinserver.max_route': server['maxRoute'], 'gremlinserver.timeout': server['timeoutSeconds'],
                  'restserver.request_timeout': server['timeoutSeconds'],
                  'restserver.connection_idle_timeout': server['idleTimeoutSeconds'],
                  'restserver.connection_max_requests': server['connectionMaxRequests'],
                  'memory_monitor.threshold': 1, 'log.slow_query_body_limit': 0}
    graph = {'gremlin.graph': 'org.apache.hugegraph.HugeFactory', 'backend': 'rocksdb', 'serializer': 'binary',
             'store': 'hugegraph', 'rocksdb.data_path': '/work/data', 'rocksdb.wal_path': '/work/data',
             'vertex.cache_capacity': 1000, 'edge.cache_capacity': 1000}
    log = ('<?xml version="1.0" encoding="UTF-8"?>\n<Configuration status="error">'
           '<Appenders><Console name="console" target="SYSTEM_OUT"><PatternLayout '
           'pattern="%d{ISO8601} [%t] %-5p %c - %m%n"/></Console></Appenders>'
           '<Loggers><Root level="info"><AppenderRef ref="console"/></Root></Loggers></Configuration>\n')
    lines = lambda values: ''.join(f'{key}={value}\n' for key, value in values.items())
    return {'conf/gremlin-server.yaml': gremlin, 'conf/rest-server.properties': lines(properties),
            'conf/graphs/hugegraph.properties': lines(graph), 'conf/log4j2.xml': log}



def formal_controls_snapshot(root, proc_root=Path('/proc')):
    """Formal experiments require host swap OFF and the HG monitor disabled.

    This observes host state; it never invokes sudo or changes host policy.
    Functional/development runs do not call this gate.
    """
    proc_root = Path(proc_root)
    raw = (proc_root / 'meminfo').read_text()
    values = re.findall(r'^SwapTotal:\s+(\d+) kB$', raw, re.M)
    require(len(values) == 1, 'formal-swap-state-unavailable')
    swaps = (proc_root / 'swaps').read_text().splitlines()
    require(swaps and swaps[0].split() == ['Filename', 'Type', 'Size', 'Used', 'Priority'],
            'formal-swap-inventory-unavailable')
    active = len([line for line in swaps[1:] if line.strip()])
    config = (Path(root) / 'conf/rest-server.properties').read_text()
    threshold = re.findall(r'^memory_monitor\.threshold=(.*)$', config, re.M)
    disabled = threshold == ['1']
    total = int(values[0]) * 1024
    return {'atUnixSeconds': time.time(), 'atMonotonicNs': time.monotonic_ns(),
            'hostBootId': (proc_root / 'sys/kernel/random/boot_id').read_text().strip(),
            'swapTotalBytes': total, 'activeSwapAreas': active,
            'memoryMonitorThreshold': 1 if disabled else None,
            'memoryMonitorDisabled': disabled, 'passed': total == 0 and active == 0 and disabled}

def freeze(settings_file, output):
    settings = read_json(settings_file, 65536)
    validate_settings(settings)
    require(os.geteuid() != 0, 'plan-as-nonroot-owner')
    output = Path(output).resolve()
    require(not output.exists(), 'attempt-directory-already-exists')
    require(not any(c in str(output) for c in '\n\r\0,'), 'invalid-attempt-path')
    sources = {}
    for name in ('distribution', 'javaHome'):
        path = Path(settings[name]).resolve(strict=True)
        require(path not in output.parents and output not in path.parents and path != output, 'attempt-overlaps-source')
        settings[name] = str(path)
        sources[name] = {'path': str(path), 'files': inventory(path)}
    dist, java = Path(settings['distribution']), Path(settings['javaHome'])
    for path in ('bin/jvm-module.options', 'conf/gremlin-server.yaml', 'conf/java-security.properties',
                 'scripts/empty-sample.groovy'):
        require((dist / path).is_file(), 'incomplete-hugegraph-distribution')
    require((java / 'bin/java').is_file() and os.access(java / 'bin/java', os.X_OK) and
            (java / 'bin/jcmd').is_file() and os.access(java / 'bin/jcmd', os.X_OK) and
            (java / 'conf/security/java.security').is_file(), 'incomplete-jdk')
    require(re.search(r'^JAVA_VERSION="17\.', (java / 'release').read_text(), re.M), 'jdk17-required')
    jars = [f['path'] for f in sources['distribution']['files']
            if f['type'] in ('file', 'symlink') and f['path'].endswith('.jar') and
            f['path'].startswith(('lib/', 'plugins/', 'ext/')) and
            not f['path'].startswith('lib/arthas/')]
    require(jars and any(Path(p).name.startswith('hugegraph') for p in jars), 'hugegraph-jars-required')
    init_classpath = sorted((p for p in jars if p.startswith(('lib/', 'plugins/'))),
                            key=lambda path: (path.startswith('plugins/'),
                                              not Path(path).name.startswith('hugegraph'), path))
    def server_order(path):
        if path.startswith('lib/'):
            name = Path(path).name
            return (0 if name.startswith('log4j-slf4j-impl') else 1 if name.startswith('hugegraph') else 2, path)
        return (3 if path.startswith('ext/') else 4, path)
    classpath = sorted(jars, key=server_order)
    jdk_observation = observation_jdk(settings, sources) if 'jvmObservation' in settings else None
    output.mkdir(mode=0o700)
    (output / 'work').mkdir(mode=0o700)
    (output / 'work/scripts').mkdir(mode=0o700)
    for name, raw in configurations(settings, (dist / 'conf/gremlin-server.yaml').read_text()).items():
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with path.open('x') as stream:
            stream.write(raw)
    shutil.copyfile(dist / 'conf/java-security.properties', output / 'conf/java-security.properties')
    for name in ('hugegraph-environment.py', 'hugegraph-service.py', 'hugegraph-mode-probe.py',
                 'jvm-clock-capture.py'):
        shutil.copyfile(Path(__file__).with_name(name), output / name)
    observation = None
    if jdk_observation:
        shutil.copyfile(Path(__file__).with_name('jvm-observation.py'), output / 'jvm-observation.py')
        temporary = output / 'jvm-observation-spec.tmp'
        write_json(temporary, settings['jvmObservation'], exclusive=True)
        try:
            generated = observation_component(output).plan(temporary, output / 'jvm-observation')
        finally:
            temporary.unlink()
        observation = {'planDirectory': 'jvm-observation', 'planSha256': generated['planSha256'],
                       'jdk': jdk_observation}
    control = [{'path': p.relative_to(output).as_posix(), 'sha256': digest(p), 'bytes': p.stat().st_size}
               for p in sorted(output.rglob('*')) if p.is_file()]
    attempt = uuid.uuid4().hex
    plan = {'schemaVersion': 1, 'kind': 'hugegraph-http-environment-plan', 'attemptId': attempt,
            'root': str(output), 'owner': {'uid': os.geteuid(), 'gid': os.getegid()}, 'settings': settings,
            'sources': sources, 'classpath': classpath, 'initClasspath': init_classpath, 'controlFiles': control,
            'containerName': 'hgbench-http-' + attempt, 'endpoint': 'http://127.0.0.1:18080/gremlin',
            'driverNetwork': 'container:<verified-container-id>', 'performanceAccepted': False,
            'budgetSemantics': 'Docker CPU/memory/PID/swap; disk polling may overshoot until shutdown'}
    if settings['purpose'] == 'calibration':
        plan['formalControlsContract'] = 'hg-monitor-off-host-swap-off-v1'
    if observation:
        plan['jvmObservation'] = observation
    write_json(output / 'plan.json', plan, exclusive=True)
    write_json(output / 'plan.lock.json', {'schemaVersion': 1, 'attemptId': attempt,
                                        'planSha256': digest(output / 'plan.json')}, exclusive=True)
    return {'attempt': str(output), 'planSha256': digest(output / 'plan.json'), 'executed': False,
            'nextCommand': ['python3', str(output / 'hugegraph-environment.py'), 'serve', '--attempt', str(output)]}


def load_attempt(root, verify_sources=False):
    root = Path(root).resolve(strict=True)
    plan = read_json(root / 'plan.json')
    lock = read_json(root / 'plan.lock.json', 65536)
    require(plan['schemaVersion'] == 1 and plan['kind'] == 'hugegraph-http-environment-plan' and
            plan['root'] == str(root) and re.fullmatch(r'[a-f0-9]{32}', plan['attemptId']) and
            plan['containerName'] == 'hgbench-http-' + plan['attemptId'] and
            lock['attemptId'] == plan['attemptId'] and lock['planSha256'] == digest(root / 'plan.json'),
            'attempt-plan-identity-mismatch')
    require(plan['owner'] == {'uid': os.geteuid(), 'gid': os.getegid()} and os.geteuid() != 0,
            'attempt-owner-mismatch')
    validate_settings(plan['settings'])
    expected_control = {'hugegraph-environment.py', 'hugegraph-service.py', 'hugegraph-mode-probe.py',
                        'jvm-clock-capture.py',
                        'conf/gremlin-server.yaml', 'conf/rest-server.properties',
                        'conf/graphs/hugegraph.properties', 'conf/log4j2.xml', 'conf/java-security.properties'}
    enabled = 'jvmObservation' in plan['settings']
    require(('jvmObservation' in plan) == enabled, 'jvm-observation-plan-presence-mismatch')
    if enabled:
        expected_control.update({'jvm-observation.py', 'jvm-observation/spec.json',
                                 'jvm-observation/plan.json', 'jvm-observation/plan.lock.json'})
        exact(plan['jvmObservation'], ('planDirectory', 'planSha256', 'jdk'), 'jvm-observation-plan')
        require(plan['jvmObservation']['planDirectory'] == 'jvm-observation',
                'jvm-observation-plan-directory-mismatch')
    require(len(plan['controlFiles']) == len(expected_control) and
            {f['path'] for f in plan['controlFiles']} == expected_control, 'control-inventory-mismatch')
    if verify_sources:
        require(not (root / 'work').is_symlink() and (root / 'work').is_dir() and
                not (root / 'work/scripts').is_symlink() and (root / 'work/scripts').is_dir(),
                'runtime-directory-identity-mismatch')
        for file in plan['controlFiles']:
            path = root / file['path']
            require(path.is_file() and not path.is_symlink() and path.stat().st_size == file['bytes'] and
                    digest(path) == file['sha256'], 'frozen-control-content-changed')
        frozen = {f['path']: f['sha256'] for f in plan['controlFiles']}
        require(digest(__file__) == frozen['hugegraph-environment.py'] and
                digest(Path(__file__).with_name('hugegraph-service.py')) == frozen['hugegraph-service.py'],
                'execute-frozen-controller-copy')
        for source in plan['sources'].values():
            verify_inventory(source['path'], source['files'])
        if enabled:
            spec, sha, _ = observation_component(root).load_plan(root / 'jvm-observation')
            require(spec == plan['settings']['jvmObservation'] and sha == plan['jvmObservation']['planSha256'] and
                    plan['jvmObservation']['jdk'] == observation_jdk(plan['settings'], plan['sources']),
                    'jvm-observation-binding-mismatch')
    return root, plan, lock['planSha256']


def docker_args(plan, *args):
    binary = shutil.which('docker')
    require(binary, 'docker-cli-unavailable')
    return [binary, '--host', 'unix://' + plan['settings']['dockerSocket'], *map(str, args)]


def docker(plan, *args, check=True):
    result = subprocess.run(docker_args(plan, *args), stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=max(30, plan['settings']['budget']['shutdownSeconds'] + 10),
                            env={key: value for key, value in os.environ.items()
                                             if not key.startswith('DOCKER_')})
    require(len(result.stdout) <= 2 * 1024**2 and len(result.stderr) <= 65536, 'docker-output-too-large')
    if check:
        require(result.returncode == 0, 'docker-command-failed: ' + result.stderr.decode(errors='replace')[:500])
    return result


def create_arguments(plan, plan_sha):
    root, settings = plan['root'], plan['settings']
    budget = settings['budget']
    args = ['create', '--name', plan['containerName'], '--cidfile', root + '/container.id', '--pull=never',
            '--label', LABEL + '.attempt=' + plan['attemptId'], '--label', LABEL + '.plan=' + plan_sha,
            '--network', 'none', '--user', f"{plan['owner']['uid']}:{plan['owner']['gid']}",
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
            '--memory', str(budget['memoryBytes']), '--memory-swap', str(budget['memoryBytes']),
            '--cpus', str(budget['cpus']), '--pids-limit', str(budget['pids']), '--restart', 'no',
            '--log-driver', 'none', '--stop-timeout', str(budget['shutdownSeconds'] + 5),
            '--tmpfs', f"/tmp:rw,exec,nosuid,nodev,size={budget['tmpfsBytes']},mode=1777",
            '--workdir', '/work', '--entrypoint', settings['python']]
    mounts = [(settings['distribution'], '/opt/hugegraph', True), (settings['javaHome'], '/opt/jdk', True),
              (root, '/control', True), (root + '/work', '/work', False),
              (settings['distribution'] + '/scripts', '/work/scripts', True)]
    for source, target, readonly in mounts:
        args += ['--mount', f"type=bind,src={source},dst={target}" + (',readonly' if readonly else '')]
    args += [settings['image'], '-B', '/control/hugegraph-service.py', '--plan', '/control/plan.json',
             '--plan-sha256', plan_sha, '--work', '/work']
    return args


def verify_container(info, plan, plan_sha, expected_id=None):
    settings, labels = plan['settings'], info.get('Config', {}).get('Labels', {})
    require(re.fullmatch(r'[a-f0-9]{64}', info.get('Id', '')) is not None and
            (expected_id is None or info['Id'] == expected_id) and
            info.get('Name') == '/' + plan['containerName'] and
            labels.get(LABEL + '.attempt') == plan['attemptId'] and labels.get(LABEL + '.plan') == plan_sha and
            info.get('Image') == settings['image'], 'container-owner-identity-mismatch')
    return info


def inspect_owned(root, plan, plan_sha, missing_ok=False):
    cid_path = root / 'container.id'
    require(not cid_path.is_symlink() and (not cid_path.exists() or cid_path.stat().st_size <= 65),
            'invalid-container-id-file')
    # Docker creates --cidfile before it creates the container, then writes its ID.
    # During that window recover only by the exact plan name and verified labels.
    expected = (cid_path.read_text().strip() or None) if cid_path.exists() else None
    require(expected is None or re.fullmatch(r'[a-f0-9]{64}', expected), 'invalid-container-id-file')
    inspected = docker(plan, 'inspect', '--type', 'container', expected or plan['containerName'], check=False)
    if inspected.returncode:
        require(missing_ok and b'no such' in inspected.stderr.lower(), 'owned-container-unavailable')
        return None
    values = json.loads(inspected.stdout)
    require(len(values) == 1, 'ambiguous-container-identity')
    return verify_container(values[0], plan, plan_sha, expected)


def verify_limits(info, plan):
    config, host = info['Config'], info['HostConfig']
    settings, owner = plan['settings'], plan['owner']
    budget = settings['budget']
    require(host.get('NetworkMode') == 'none' and not host.get('PortBindings') and
            not host.get('PublishAllPorts') and
            not any((info.get('NetworkSettings', {}).get('Ports') or {}).values()) and
            host.get('ReadonlyRootfs') is True and
            host.get('Memory') == budget['memoryBytes'] and host.get('MemorySwap') == budget['memoryBytes'] and
            host.get('NanoCpus') == round(budget['cpus'] * 1000000000) and
            host.get('PidsLimit') == budget['pids'] and set(host.get('CapDrop') or []) == {'ALL'} and
            not host.get('CapAdd') and not host.get('Privileged') and
            'no-new-privileges:true' in host.get('SecurityOpt', []) and
            host.get('RestartPolicy', {}).get('Name') == 'no' and
            host.get('PidMode', '') == '' and not host.get('Devices') and not host.get('VolumesFrom') and
            host.get('LogConfig', {}).get('Type') == 'none' and
            config.get('User') == f"{owner['uid']}:{owner['gid']}" and
            config.get('WorkingDir') == '/work' and config.get('Entrypoint') == [settings['python']],
            'docker-resource-or-isolation-mismatch')
    expected = {'/opt/hugegraph': (settings['distribution'], False), '/opt/jdk': (settings['javaHome'], False),
                '/control': (plan['root'], False), '/work': (plan['root'] + '/work', True),
                '/work/scripts': (settings['distribution'] + '/scripts', False)}
    actual = {m['Destination']: (m['Source'], m['RW']) for m in info['Mounts'] if m['Type'] == 'bind'}
    require(actual == expected, 'container-mount-identity-mismatch')
    require(config.get('Cmd') == ['-B', '/control/hugegraph-service.py', '--plan', '/control/plan.json',
                                  '--plan-sha256', digest(Path(plan['root']) / 'plan.json'), '--work', '/work'],
            'container-command-identity-mismatch')
    require(host.get('Tmpfs') == {'/tmp': f"rw,exec,nosuid,nodev,size={budget['tmpfsBytes']},mode=1777"},
            'container-tmpfs-budget-mismatch')


def status(attempt):
    root, plan, plan_sha = load_attempt(attempt)
    dispatched = (root / 'dispatch.json').exists() or (root / 'container.id').exists()
    info = inspect_owned(root, plan, plan_sha, missing_ok=True) if dispatched else None
    result = {'attemptId': plan['attemptId'], 'planSha256': plan_sha,
              'state': 'planned' if not (root / 'dispatch.json').exists() else 'dispatch-recorded',
              'performanceAccepted': False, 'container': None}
    if not dispatched and (root / 'stop-request.json').exists():
        result['state'] = 'stopped-before-dispatch'
    if info:
        verify_limits(info, plan)
        result['state'] = info['State']['Status']
        result['container'] = {'id': info['Id'], 'state': info['State'],
                               'driverNetwork': 'container:' + info['Id']}
    for name in ('service-status', 'ready', 'service-result'):
        path = root / 'work' / (name + '.json')
        if path.exists():
            record = read_json(path, 65536)
            require(record.get('attemptId') == plan['attemptId'] and record.get('planSha256') == plan_sha,
                    'service-record-identity-mismatch')
            result[name] = record
    return result


def stop(attempt):
    root, plan, plan_sha = load_attempt(attempt)
    # Publish intent atomically before inspecting Docker, closing the create/start race.
    marker = root / 'stop-request.json'
    temporary = root / ('stop-request.' + uuid.uuid4().hex + '.tmp')
    write_json(temporary, {'attemptId': plan['attemptId'], 'planSha256': plan_sha,
                           'atUnixSeconds': time.time()}, exclusive=True)
    try:
        try:
            os.link(temporary, marker)
        except FileExistsError:
            pass
    finally:
        temporary.unlink()
    require(SERVICE.stop_requested(root, plan['attemptId'], plan_sha), 'stop-marker-unavailable')
    dispatched = (root / 'dispatch.json').exists() or (root / 'container.id').exists()
    info = inspect_owned(root, plan, plan_sha, missing_ok=True) if dispatched else None
    if info and info['State']['Running']:
        docker(plan, 'stop', '--time', str(plan['settings']['budget']['shutdownSeconds'] + 5), info['Id'])
    elif info and info['State']['Status'] == 'created':
        # An interrupted create/start boundary stays unstarted; stop never dispatches it.
        pass
    return status(root)


def serve(attempt):
    root, plan, plan_sha = load_attempt(attempt, verify_sources=True)
    require(not (root / 'stop-request.json').exists(), 'attempt-already-stopped')
    require(set(p.name for p in (root / 'work').iterdir()) == {'scripts'} and
            not list((root / 'work/scripts').iterdir()), 'runtime-directory-not-empty')
    require(not SERVICE.budget_reason(SERVICE.directory_usage(root / 'work'), plan['settings']['budget']),
            'initial-disk-budget-failed')
    write_json(root / 'dispatch.json', {'attemptId': plan['attemptId'], 'planSha256': plan_sha,
                                      'atUnixSeconds': time.time()}, exclusive=True)
    cancel = [False]
    previous = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, lambda _signum, _frame: cancel.__setitem__(0, True))
    created = False
    started = time.monotonic()
    error = None
    controls = None
    if plan.get('formalControlsContract'):
        require(plan['formalControlsContract'] == 'hg-monitor-off-host-swap-off-v1',
                'unsupported-formal-controls-contract')
        controls = {'schemaVersion': 1, 'kind': 'http-formal-environment-controls',
                    'contract': plan['formalControlsContract'], 'attemptId': plan['attemptId'],
                    'planSha256': plan_sha, 'checks': 0, 'maximumSwapTotalBytes': 0,
                    'maximumActiveSwapAreas': 0, 'allPassed': True, 'first': None, 'last': None,
                    'scope': 'Host polling before launch, during supervision and after cleanup; no continuous proof'}
    def check_controls():
        if controls is None:
            return
        record = formal_controls_snapshot(root)
        controls['checks'] += 1
        controls['first'] = controls['first'] or record
        controls['last'] = record
        controls['maximumSwapTotalBytes'] = max(controls['maximumSwapTotalBytes'], record['swapTotalBytes'])
        controls['maximumActiveSwapAreas'] = max(controls['maximumActiveSwapAreas'], record['activeSwapAreas'])
        controls['allPassed'] = controls['allPassed'] and record['passed']
        require(record['passed'], 'formal-requires-memory-monitor-disabled-and-host-swap-off')
    try:
        check_controls()
        image = json.loads(docker(plan, 'image', 'inspect', plan['settings']['image']).stdout)
        require(len(image) == 1 and image[0]['Id'] == plan['settings']['image'], 'docker-image-identity-mismatch')
        require(not cancel[0] and not (root / 'stop-request.json').exists(), 'cancelled-before-create')
        docker(plan, *create_arguments(plan, plan_sha))
        created = True
        info = inspect_owned(root, plan, plan_sha)
        verify_limits(info, plan)
        write_json(root / 'container-before-start.json', info, exclusive=True)
        require(not cancel[0] and not (root / 'stop-request.json').exists(), 'cancelled-before-start')
        docker(plan, 'start', info['Id'])
        while True:
            check_controls()
            info = inspect_owned(root, plan, plan_sha)
            if not info['State']['Running']:
                break
            if (cancel[0] or (root / 'stop-request.json').exists() or
                    time.monotonic() - started > plan['settings']['budget']['wallSeconds'] + 30):
                if time.monotonic() - started > plan['settings']['budget']['wallSeconds'] + 30:
                    error = 'supervisor-wall-clock-budget'
                docker(plan, 'stop', '--time', str(plan['settings']['budget']['shutdownSeconds'] + 5), info['Id'])
                break
            time.sleep(plan['settings']['budget']['pollSeconds'])
    except Exception as failure:
        error = str(failure)[:1000]
    finally:
        # Recover exact name if Docker create completed but its response was interrupted.
        try:
            info = inspect_owned(root, plan, plan_sha, missing_ok=True)
            if info and info['State']['Running']:
                docker(plan, 'stop', '--time', str(plan['settings']['budget']['shutdownSeconds'] + 5), info['Id'])
            if info:
                write_json(root / 'container-final.json', inspect_owned(root, plan, plan_sha), exclusive=True)
        except Exception as cleanup:
            error = (error or '') + '; cleanup: ' + str(cleanup)[:500]
        if controls is not None:
            try:
                check_controls()
            except Exception as failure:
                controls['allPassed'] = False
                controls['error'] = str(failure)[:500]
                error = error or str(failure)[:500]
            write_json(root / 'formal-controls.json', controls, exclusive=True)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    try:
        observed = status(root)
    except Exception as failure:
        observed = {'state': 'unavailable', 'error': str(failure)[:500]}
        error = error or 'terminal-status-unavailable'
    terminal = observed.get('service-result')
    container = observed.get('container')
    if (not terminal or terminal.get('error') or not terminal.get('outcomes') or
            not SERVICE.unforced_stop(terminal['outcomes'][-1]) or not observed.get('ready') or
            not container or container['state'].get('Running') or container['state'].get('OOMKilled') or
            container['state'].get('ExitCode') != 0):
        error = error or 'environment-did-not-end-with-verified-unforced-stop'
    result = {'schemaVersion': 1, 'kind': 'hugegraph-http-environment-result', 'attemptId': plan['attemptId'],
              'planSha256': plan_sha, 'created': created, 'error': error,
              'elapsedSeconds': time.monotonic() - started, 'performanceAccepted': False}
    write_json(root / 'supervisor-result.json', result, exclusive=True)
    result['status'] = observed
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    plan = commands.add_parser('plan', help='Offline: freeze settings, complete distribution/JDK and generated config')
    plan.add_argument('--settings', type=Path, required=True,
                      help='Exact schema documented in http-controlled-environment.md')
    plan.add_argument('--out', type=Path, required=True, help='New attempt directory; parent must exist')
    for name in ('serve', 'status', 'stop'):
        command = commands.add_parser(name, help={'serve': 'Foreground, one dispatch only; independent container TTL',
                                                  'status': 'Read exact owned container and service records',
                                                  'stop': 'Stop only the exact owned container; keep evidence'}[name])
        command.add_argument('--attempt', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'plan':
            data = freeze(args.settings, args.out)
        else:
            data = {'serve': serve, 'status': status, 'stop': stop}[args.command](args.attempt)
        failed = bool(data.get('error'))
        print(json.dumps({'schemaVersion': 1, 'ok': not failed, 'command': args.command, 'data': data}, sort_keys=True))
        return 6 if failed else 0
    except Exception as error:
        print(json.dumps({'schemaVersion': 1, 'ok': False, 'command': args.command,
                          'error': str(error)[:1500]}, sort_keys=True))
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
