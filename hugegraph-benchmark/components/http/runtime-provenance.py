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

"""Explicit Linux runtime provenance probe and offline verifier. No target/server/HTTP operations.

Only fixed observers are executed. A probe owns one separately labeled container.
Its external expectations are for subsequent experiment freezing, never retroactive qualification.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
_SPEC = importlib.util.spec_from_file_location('provenance_common', HERE / 'comparison-workflow.py')
COMMON = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(COMMON)
need, exact, integer, read_json, read_bytes, save = (COMMON.need, COMMON.exact, COMMON.integer,
                                                   COMMON.read_json, COMMON.read_bytes, COMMON.save)
MiB, FLAGS = 1024**2, ('LD_PRELOAD', 'LD_AUDIT', 'LD_LIBRARY_PATH', 'NODE_OPTIONS', 'PYTHONHOME', 'PYTHONPATH',
                      'PYTHONSTARTUP', 'PYTHONINSPECT', 'PYTHONUSERBASE')
SOURCES = ('runtime-provenance.py', 'runtime-provenance-worker.py', 'runtime-provenance-node.mjs',
           'comparison-workflow.py')
ROLES, NS = ('host-python', 'host-node', 'driver-python'), ('time', 'time_for_children', 'pid', 'mnt', 'user')
LABEL, CANCELLED = 'org.apache.hugegraph.hgbench.runtime-probe', False
SHA, TOKEN = r'[a-f0-9]{64}', r'[a-f0-9]{32}'


def cancel(_signal, _frame):
    global CANCELLED
    CANCELLED = True


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def value_hash(value):
    return digest(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())


def validate_spec(value):
    exact(value, ('schemaVersion', 'kind', 'sourceOrigin', 'budgets', 'container'), 'PROVENANCE_SPEC_FIELDS')
    need(type(value['schemaVersion']) is int and value['schemaVersion'] == 1 and
         value['kind'] == 'http-runtime-provenance-spec' and value['sourceOrigin'] == 'linux-procfs',
         'PROVENANCE_SPEC_IDENTITY')
    budgets, container = value['budgets'], value['container']
    bounds = {'wallMs': (5000, 120000), 'commandMs': (500, 15000), 'handshakeMs': (500, 20000),
        'cleanupMs': (1000, 10000), 'maxRawBytes': (65536, MiB), 'maxEvidenceBytes': (MiB, 16 * MiB),
        'maxExecutableBytes': (MiB, 512 * MiB), 'maxHashBytes': (64 * MiB, 4 * 1024 * MiB)}
    exact(budgets, bounds, 'PROVENANCE_BUDGET_FIELDS')
    for key, limits in bounds.items():
        integer(budgets[key], *limits, 'PROVENANCE_' + key)
    need(3 * budgets['handshakeMs'] <= budgets['wallMs'] and
         budgets['maxRawBytes'] < budgets['maxEvidenceBytes'], 'PROVENANCE_BUDGET_CONSISTENCY')
    exact(container, ('cpus', 'memoryBytes', 'pids', 'tmpfsBytes'), 'PROVENANCE_CONTAINER_FIELDS')
    need(type(container['cpus']) in (int, float) and math.isfinite(container['cpus']) and
         .1 <= container['cpus'] <= 1, 'PROVENANCE_CPUS')
    for key, low, high in [('memoryBytes', 64 * MiB, 512 * MiB), ('pids', 16, 64),
                           ('tmpfsBytes', 4 * MiB, 32 * MiB)]:
        integer(container[key], low, high, 'PROVENANCE_' + key)


def runtime_binding(runtime_path):
    path = Path(runtime_path)
    runtime, lock = read_json(path, 16 * MiB), read_json(path.with_name('runtime.lock.json'))
    need(path.name == 'runtime.json' and lock.get('schemaVersion') == 1 and
         lock.get('kind') == 'http-comparison-runtime-lock' and
         lock.get('runtime') == COMMON.file_entry(path, 'runtime.json') and
         runtime.get('schemaVersion') == 1 and runtime.get('kind') == 'http-comparison-runtime-plan' and
         lock.get('planDigest') == runtime.get('planDigest'), 'PROVENANCE_RUNTIME_LOCK')
    COMMON.validate_runtime(runtime['settings'])
    settings, identities = runtime['settings'], runtime['identities']
    selected = {'host-python': identities['hostPython'], 'host-node':
                next(item for item in identities['node'] if item['path'] == 'bin/node')}
    need(selected['host-python']['path'] == settings['hostPython'] and
         selected['host-node'].get('type') == 'file', 'PROVENANCE_SELECTED_EXECUTABLE')
    for item in selected.values():
        need(type(item['bytes']) is int and item['bytes'] > 0 and re.fullmatch(SHA, item['sha256']),
             'PROVENANCE_RUNTIME_EXECUTABLE')
    need(re.fullmatch(SHA, identities['dockerExecutable']['sha256']) and
         identities['dockerExecutable']['path'] == settings['dockerExecutable'] and
         re.fullmatch('sha256:' + SHA, settings['driver']['image']), 'PROVENANCE_RUNTIME_DOCKER')
    return runtime, lock, {'runtimeDigest': digest(read_bytes(path.with_name('runtime.lock.json'))),
        'runtimeFileSha256': digest(read_bytes(path, 16 * MiB)), 'planDigest': runtime['planDigest'],
        'imageId': settings['driver']['image'], 'selected': selected}


class Budget:
    def __init__(self, spec):
        self.spec, self.started = spec, time.monotonic()
        self.deadline = self.started + spec['budgets']['wallMs'] / 1000
        self.cleanup_deadline = None
        self.stop_path, self.stop_binding = None, None
        self.raw_bytes, self.hash_bytes = 0, 0

    def check(self):
        if self.cleanup_deadline is not None:
            need(time.monotonic() < self.cleanup_deadline, 'PROVENANCE_CLEANUP_DEADLINE')
        else:
            need(not CANCELLED and time.monotonic() < self.deadline, 'PROVENANCE_DEADLINE_OR_CANCELLED')
            if self.stop_path is not None and self.stop_path.exists():
                need(read_json(self.stop_path) == self.stop_binding, 'PROVENANCE_STOP_IDENTITY')
                need(False, 'PROVENANCE_STOP_REQUESTED')

    def raw(self, count):
        self.raw_bytes += count
        cap = self.spec['budgets']['maxRawBytes'] - (32768 if self.cleanup_deadline is None else 0)
        need(self.raw_bytes <= cap, 'PROVENANCE_RAW_BUDGET')

    def hash(self, count):
        self.hash_bytes += count
        need(self.hash_bytes <= self.spec['budgets']['maxHashBytes'], 'PROVENANCE_HASH_BUDGET')


def file_identity(path, budget, executable=False):
    budget.check()
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        cap = budget.spec['budgets']['maxExecutableBytes'] if executable else 16 * MiB
        need(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= cap, 'PROVENANCE_FILE_TYPE_OR_BUDGET')
        value = {'path': str(path), 'bytes': before.st_size, 'device': str(before.st_dev),
                 'inode': str(before.st_ino), 'mtimeNs': str(before.st_mtime_ns), 'ctimeNs': str(before.st_ctime_ns)}
        sha, count = hashlib.sha256(), 0
        while True:
            budget.check()
            block = stream.read(MiB)
            if not block:
                break
            count += len(block)
            budget.hash(len(block))
            need(count <= before.st_size, 'PROVENANCE_FILE_CHANGED')
            sha.update(block)
        after = os.fstat(stream.fileno())
        need(count == before.st_size and (before.st_dev, before.st_ino, before.st_size,
             before.st_mtime_ns, before.st_ctime_ns) == (after.st_dev, after.st_ino, after.st_size,
             after.st_mtime_ns, after.st_ctime_ns), 'PROVENANCE_FILE_CHANGED')
        return {**value, 'sha256': sha.hexdigest()}


def text(path, cap, budget):
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(cap + 1)
    need(len(raw) <= cap, 'PROVENANCE_PROC_BUDGET')
    budget.raw(len(raw))
    return raw


def proc_observation(pid, budget):
    root = Path('/proc') / str(pid)
    first = text(root / 'stat', 4096, budget).decode()
    executable = file_identity(root / 'exe', budget, True)
    ns = {name: os.readlink(root / 'ns' / name) for name in NS}
    offsets = text(root / 'timens_offsets', 256, budget).decode()
    cmdline = text(root / 'cmdline', 8192, budget)
    status = text(root / 'status', 16384, budget).decode()
    boot_raw = text('/proc/sys/kernel/random/boot_id', 128, budget).decode()
    last = text(root / 'stat', 4096, budget).decode()
    return {'pid': pid, 'statBeforeRaw': first, 'statAfterRaw': last, 'executable': executable,
            'namespaces': ns, 'timeOffsetsRaw': offsets, 'cmdlineBase64': base64.b64encode(cmdline).decode(),
            'statusRaw': status, 'bootId': boot_raw.strip(), 'bootIdRaw': boot_raw, 'observedMonotonicNs': COMMON.ns()}


def injection_flags(values):
    # Values remain in memory only, never in logs or evidence.
    fields = dict(value.split('=', 1) for value in values if isinstance(value, str) and '=' in value)
    return {key: bool(fields.get(key)) for key in FLAGS}


def safe_image(value):
    return {key: value.get(key) for key in ('Id', 'Architecture', 'Os')} | {
        'injectionEnvironmentPresent': injection_flags(value.get('Config', {}).get('Env') or [])}


def safe_container(value):
    config, host, state = value.get('Config', {}), value.get('HostConfig', {}), value.get('State', {})
    return {'Id': value.get('Id'), 'Name': value.get('Name'), 'Image': value.get('Image'),
        'State': {key: state.get(key) for key in ('Running', 'Paused', 'Restarting', 'OOMKilled', 'Dead', 'Pid', 'ExitCode')},
        'Config': {key: config.get(key) for key in ('Entrypoint', 'Cmd', 'User', 'WorkingDir')} | {
            'Labels': {key: item for key, item in (config.get('Labels') or {}).items()
                       if key in (LABEL + '.attempt', LABEL + '.plan')},
            'injectionEnvironmentPresent': injection_flags(config.get('Env') or [])},
        'HostConfig': {key: host.get(key) for key in ('NetworkMode', 'ReadonlyRootfs', 'Memory', 'MemorySwap',
            'NanoCpus', 'PidsLimit', 'CapDrop', 'CapAdd', 'Privileged', 'SecurityOpt', 'RestartPolicy', 'PidMode',
            'Devices', 'VolumesFrom', 'LogConfig', 'Tmpfs', 'PortBindings', 'PublishAllPorts')},
        'Mounts': [{key: item.get(key) for key in ('Type', 'Source', 'Destination', 'RW')}
                   for item in value.get('Mounts', [])]}


def worker_argv(plan, root, role):
    settings = plan['runtimeSettings']
    if role == 'host-node':
        return [str(Path(settings['nodeHome']) / 'bin/node'), str(root / 'control/runtime-provenance-node.mjs'),
                '--request', str(root / 'control/host-node.json')]
    if role == 'host-python':
        return [settings['hostPython'], '-I', '-S', '-B', str(root / 'control/runtime-provenance-worker.py'),
                '--request', str(root / 'control/host-python.json')]
    return [settings['driver']['python'], '-I', '-S', '-B', '/probe/runtime-provenance-worker.py',
            '--request', '/probe/driver-python.json']


def verify_owned(value, plan, plan_sha):
    labels = value['Config']['Labels'] or {}
    need(re.fullmatch(SHA, value.get('Id') or '') and value['Name'] == '/' + plan['containerName'] and
         value['Image'] == plan['binding']['imageId'] and labels.get(LABEL + '.attempt') == plan['attemptId'] and
         labels.get(LABEL + '.plan') == plan_sha, 'PROVENANCE_CONTAINER_IDENTITY')
    return value


def verify_limits(value, plan, root):
    host, config = value['HostConfig'], value['Config']
    container = plan['spec']['container']
    need(host['NetworkMode'] == 'none' and host['ReadonlyRootfs'] is True and
         host['Memory'] == host['MemorySwap'] == container['memoryBytes'] and
         host['NanoCpus'] == round(container['cpus'] * 1e9) and host['PidsLimit'] == container['pids'] and
         set(host['CapDrop'] or []) == {'ALL'} and not host['CapAdd'] and not host['Privileged'] and
         'no-new-privileges:true' in (host['SecurityOpt'] or []) and host['RestartPolicy']['Name'] == 'no' and
         not host['PidMode'] and not host['Devices'] and not host['VolumesFrom'] and
         host['LogConfig']['Type'] == 'none' and not host['PortBindings'] and not host['PublishAllPorts'] and
         config['User'] == str(plan['owner']['uid']) + ':' + str(plan['owner']['gid']) and
         config['WorkingDir'] == '/tmp' and config['Entrypoint'] == [plan['runtimeSettings']['driver']['python']] and
         config['Cmd'] == worker_argv(plan, root, 'driver-python')[1:] and
         not any(config['injectionEnvironmentPresent'].values()) and
         host['Tmpfs'] == {'/tmp': 'rw,noexec,nosuid,nodev,size=' + str(container['tmpfsBytes']) + ',mode=1777'} and
         value['Mounts'] == [{'Type': 'bind', 'Source': str(root / 'control'), 'Destination': '/probe', 'RW': False}],
         'PROVENANCE_CONTAINER_LIMITS')


class Docker:
    def __init__(self, root, plan, budget):
        self.root, self.plan, self.budget, self.sequence = root, plan, budget, 0
        self.record_root = root / 'docker'
        self.plan_sha = COMMON.file_entry(root / 'plan.json')['sha256']

    def call(self, args, check=True, inspect_kind=None):
        self.sequence += 1
        argv = [self.plan['runtimeSettings']['dockerExecutable'], '--host', 'unix:///var/run/docker.sock', *args]
        end = self.budget.cleanup_deadline or self.budget.deadline
        seconds = min(self.plan['spec']['budgets']['commandMs'] / 1000, end - time.monotonic())
        need(seconds > 0, 'PROVENANCE_COMMAND_DEADLINE')
        try:
            raw = COMMON.command(argv, seconds, min(262144, self.plan['spec']['budgets']['maxRawBytes']),
                                 {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'}, self.budget.check, None, False)
        except BaseException as error:
            save(self.record_root / ('command-' + str(self.sequence).zfill(3) + '.json'),
                 {'argv': argv, 'exitCode': None, 'error': str(error)[:128] if isinstance(error, COMMON.WorkflowError)
                  else type(error).__name__, 'outputPreserved': False})
            raise
        self.budget.raw(len(raw['stdout'].encode()) + len(raw['stderr'].encode()))
        receipt = {key: raw[key] for key in ('argv', 'exitCode', 'error', 'startedMonotonicNs', 'finishedMonotonicNs')}
        receipt.update(stdoutBytes=len(raw['stdout'].encode()), stderrBytes=len(raw['stderr'].encode()))
        parsed = None
        if inspect_kind and raw['exitCode'] == 0:
            data = json.loads(raw['stdout'])
            need(type(data) is list and len(data) == 1, 'PROVENANCE_DOCKER_INSPECT')
            parsed = {'image': safe_image, 'container': safe_container}[inspect_kind](data[0])
            receipt['inspection'] = parsed
        elif raw['exitCode'] == 0:
            receipt['stdoutSha256'] = digest(raw['stdout'].encode())
        receipt['notFound'] = raw['exitCode'] != 0 and 'no such' in raw['stderr'].lower()
        save(self.record_root / ('command-' + str(self.sequence).zfill(3) + '.json'), receipt)
        if check:
            need(raw['exitCode'] == 0, 'PROVENANCE_DOCKER_COMMAND')
        return parsed, receipt

    def owned(self, missing=False):
        cid = None
        if (self.root / 'container.id').exists():
            cid = read_bytes(self.root / 'container.id', 65).decode().strip() or None
            need(cid is None or re.fullmatch(SHA, cid), 'PROVENANCE_CIDFILE')
        value, receipt = self.call(['inspect', '--type', 'container', cid or self.plan['containerName']], False, 'container')
        if value is None:
            need(missing and receipt['notFound'], 'PROVENANCE_OWNED_CONTAINER_UNAVAILABLE')
            return None
        value = verify_owned(value, self.plan, self.plan_sha)
        need(cid is None or value['Id'] == cid, 'PROVENANCE_CONTAINER_REPLACED')
        return value

    def create_args(self):
        plan, limits = self.plan, self.plan['spec']['container']
        argv = ['create', '-i', '--name', plan['containerName'], '--cidfile', str(self.root / 'container.id'),
            '--pull=never', '--label', LABEL + '.attempt=' + plan['attemptId'], '--label', LABEL + '.plan=' + self.plan_sha,
            '--network', 'none', '--read-only', '--user', str(plan['owner']['uid']) + ':' + str(plan['owner']['gid']),
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--memory', str(limits['memoryBytes']),
            '--memory-swap', str(limits['memoryBytes']), '--cpus', str(limits['cpus']), '--pids-limit', str(limits['pids']),
            '--restart', 'no', '--log-driver', 'none', '--stop-timeout', '1', '--tmpfs',
            '/tmp:rw,noexec,nosuid,nodev,size=' + str(limits['tmpfsBytes']) + ',mode=1777', '--workdir', '/tmp',
            '--mount', 'type=bind,src=' + str(self.root / 'control') + ',dst=/probe,readonly',
            '--entrypoint', plan['runtimeSettings']['driver']['python'], plan['binding']['imageId'],
            *worker_argv(plan, self.root, 'driver-python')[1:]]
        return argv

    def create(self):
        self.call(self.create_args())
        value = self.owned()
        need(read_bytes(self.root / 'container.id', 65).decode().strip() == value['Id'], 'PROVENANCE_CIDFILE')
        verify_limits(value, self.plan, self.root)
        save(self.root / 'container-created.json', value)
        return value['Id']

    def cleanup(self):
        value = self.owned(True)
        if value is None:
            return {'containerRemoved': True, 'scope': 'exact probe name/labels/image absent'}
        cid = value['Id']
        if value['State']['Running']:
            self.call(['stop', '--time', '1', cid])
        self.call(['rm', '-f', cid])
        need(self.owned(True) is None, 'PROVENANCE_REMOVAL_UNPROVEN')
        return {'containerRemoved': True, 'containerId': cid, 'scope': 'exact labeled probe container only'}


class Observer:
    def __init__(self, argv, budget):
        self.budget = budget
        self.child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'LANG': 'C'}, start_new_session=True)
        self.selector = selectors.DefaultSelector()
        self.buffer, self.stderr_bytes, self.stdout_bytes = bytearray(), 0, 0
        for stream, name in ((self.child.stdout, 'out'), (self.child.stderr, 'err')):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, name)
        self.frames = []

    def pump(self, deadline):
        self.budget.check()
        need(time.monotonic() < deadline, 'PROVENANCE_HANDSHAKE_DEADLINE')
        for key, _ in self.selector.select(min(.1, max(0, deadline - time.monotonic()))):
            raw = os.read(key.fd, 32769)
            if not raw:
                self.selector.unregister(key.fileobj)
            else:
                self.budget.raw(len(raw))
                if key.data == 'out':
                    self.stdout_bytes += len(raw)
                    self.buffer.extend(raw)
                    need(len(self.buffer) <= 32768, 'PROVENANCE_FRAME_BUDGET')
                else:
                    self.stderr_bytes += len(raw)
                    need(self.stderr_bytes <= 4096, 'PROVENANCE_STDERR_BUDGET')

    def receive(self):
        end = min(self.budget.deadline, time.monotonic() + self.budget.spec['budgets']['handshakeMs'] / 1000)
        while b'\n' not in self.buffer:
            need(self.child.poll() is None or self.selector.get_map(), 'PROVENANCE_OBSERVER_EOF')
            self.pump(end)
        count = self.buffer.index(b'\n') + 1
        raw = bytes(self.buffer[:count])
        del self.buffer[:count]
        need(not self.buffer, 'PROVENANCE_UNSOLICITED_FRAME')
        value = json.loads(raw, object_pairs_hook=COMMON.unique)
        self.frames.append(raw.decode())
        return value

    def send(self, kind, token):
        self.budget.check()
        raw = json.dumps({'kind': kind, 'token': token}).encode() + b'\n'
        os.write(self.child.stdin.fileno(), raw)

    def finish(self):
        self.child.stdin.close()
        end = min(self.budget.deadline, time.monotonic() + self.budget.spec['budgets']['handshakeMs'] / 1000)
        while self.child.poll() is None or self.selector.get_map():
            self.pump(end)
        need(self.child.wait(timeout=1) == 0 and not self.buffer and not self.stderr_bytes,
             'PROVENANCE_OBSERVER_EXIT')

    def close(self):
        try:
            os.killpg(self.child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self.child.wait(timeout=.5)
        except subprocess.TimeoutExpired:
            pass
        COMMON.kill_group(self.child)  # Also reap/kill descendants after a leader exits early.
        self.selector.close()
        for stream in (self.child.stdin, self.child.stdout, self.child.stderr):
            stream.close()


def collect_role(root, plan, role, budget, docker, cid=None):
    argv = worker_argv(plan, root, role)
    if role == 'driver-python':
        argv = [plan['runtimeSettings']['dockerExecutable'], '--host', 'unix:///var/run/docker.sock',
                'start', '--attach', '--interactive', cid]
    observer, record = None, {'role': role, 'argv': argv, 'status': 'failed'}
    try:
        observer = Observer(argv, budget)
        ready = observer.receive()
        if role == 'driver-python':
            live = docker.owned()
            need(live['Id'] == cid and live['State']['Running'] is True, 'PROVENANCE_DRIVER_NOT_RUNNING')
            verify_limits(live, plan, root)
            save(root / 'container-live.json', live)
            pid = live['State']['Pid']
        else:
            pid = observer.child.pid
        record['actualPidSource'] = 'docker-inspect-State.Pid' if cid else 'owned-Popen.pid'
        record['before'] = proc_observation(pid, budget)
        budget.hash(ready['executable']['bytes'])  # Fixed observer itself hashed this many executable bytes once.
        observer.send('observe', plan['attemptId'])
        final = observer.receive()
        record['after'] = proc_observation(pid, budget)
        observer.send('finish', plan['attemptId'])
        observer.finish()
        record.update(status='completed', ready=ready, final=final, childExitCode=observer.child.returncode)
    finally:
        if observer:
            record.update(frames=observer.frames, stdoutBytes=observer.stdout_bytes, stderrBytes=observer.stderr_bytes)
            observer.close()
        save(root / 'roles' / (role + '.json'), record)
    return record


def freeze(spec_file, runtime_path, output, budget):
    root = Path(output).resolve()
    need(not root.exists(), 'PROVENANCE_NEW_OUTPUT_REQUIRED')
    runtime, lock, binding = runtime_binding(runtime_path)
    need(root != Path(runtime_path).parent and not root.is_relative_to(Path(runtime_path).parent),
         'PROVENANCE_OUTPUT_OVERLAPS_RUNTIME')
    raw_spec, raw_runtime = read_bytes(spec_file, 65536), read_bytes(runtime_path, 16 * MiB)
    needed = len(raw_runtime) + len(raw_spec) + sum((HERE / name).stat().st_size for name in SOURCES)
    need(needed + budget.spec['budgets']['maxRawBytes'] * 2 + 262144 <= budget.spec['budgets']['maxEvidenceBytes'],
         'PROVENANCE_EVIDENCE_RESERVE')
    root.mkdir(mode=0o700)
    for directory in ('control', 'roles', 'docker'):
        (root / directory).mkdir()
    (root / 'spec.json').write_bytes(raw_spec)
    (root / 'runtime.json').write_bytes(raw_runtime)
    shutil.copyfile(Path(runtime_path).with_name('runtime.lock.json'), root / 'runtime.lock.json')
    for name in SOURCES:
        shutil.copyfile(HERE / name, root / 'control' / name)
    token = uuid.uuid4().hex
    plan = {'schemaVersion': 1, 'kind': 'http-runtime-provenance-plan', 'attemptId': token, 'root': str(root),
        'owner': {'uid': os.geteuid(), 'gid': os.getegid()}, 'spec': budget.spec, 'binding': binding,
        'runtimeSettings': runtime['settings'], 'containerName': 'hgbench-runtime-' + token,
        'sources': [COMMON.file_entry(root / 'control' / name, 'control/' + name) for name in SOURCES],
        'resourceSemantics': 'container CPU/memory/pids hard limits; fixed sequential host workers, streamed bounded hashing'}
    for role in ROLES:
        save(root / 'control' / (role + '.json'), {'schemaVersion': 1, 'kind': 'http-runtime-observer-request',
            'role': role, 'token': token, 'runtimeDigest': binding['runtimeDigest'],
            **{key: budget.spec['budgets'][key] for key in ('wallMs', 'handshakeMs', 'maxExecutableBytes')}})
    save(root / 'plan.json', plan)
    save(root / 'plan.lock.json', {'schemaVersion': 1, 'kind': 'http-runtime-provenance-plan-lock',
                                 'planSha256': COMMON.file_entry(root / 'plan.json')['sha256']})
    return root, plan


def load_proof(proof):
    root = Path(proof).resolve(strict=True)
    plan = read_json(root / 'plan.json')
    validate_spec(plan['spec'])
    need(plan.get('schemaVersion') == 1 and plan.get('kind') == 'http-runtime-provenance-plan' and
         re.fullmatch(TOKEN, plan['attemptId']) and plan['containerName'] == 'hgbench-runtime-' + plan['attemptId'] and
         read_json(root / 'plan.lock.json') == {'schemaVersion': 1, 'kind': 'http-runtime-provenance-plan-lock',
             'planSha256': COMMON.file_entry(root / 'plan.json')['sha256']}, 'PROVENANCE_PLAN_LOCK')
    runtime, _, binding = runtime_binding(root / 'runtime.json')
    need(binding == plan['binding'] and runtime['settings'] == plan['runtimeSettings'] and
         read_json(root / 'spec.json') == plan['spec'], 'PROVENANCE_PLAN_DERIVATION')
    need([item['path'] for item in plan['sources']] == ['control/' + name for name in SOURCES],
         'PROVENANCE_SOURCE_INVENTORY')
    for item in plan['sources']:
        need(COMMON.file_entry(root / item['path'], item['path']) == item, 'PROVENANCE_SOURCE_CHANGED')
    for role in ROLES:
        need(read_json(root / 'control' / (role + '.json')) == {
            'schemaVersion': 1, 'kind': 'http-runtime-observer-request', 'role': role, 'token': plan['attemptId'],
            'runtimeDigest': binding['runtimeDigest'],
            **{key: plan['spec']['budgets'][key] for key in ('wallMs', 'handshakeMs', 'maxExecutableBytes')}},
            'PROVENANCE_FROZEN_REQUEST_CHANGED')
    return root, plan


def probe(spec_file, runtime_path, output):
    spec = read_json(spec_file, 65536)
    validate_spec(spec)
    need(sys.platform == 'linux' and os.geteuid() != 0, 'PROVENANCE_NONROOT_LINUX_REQUIRED')
    budget = Budget(spec)
    root, plan = freeze(spec_file, runtime_path, output, budget)
    budget.stop_path = root / 'stop-request.json'
    budget.stop_binding = {'attemptId': plan['attemptId'], 'planSha256': COMMON.file_entry(root / 'plan.json')['sha256']}
    docker = Docker(root, plan, budget)
    failure, cleanup, records = None, None, {}
    save(root / 'dispatch.json', {'attemptId': plan['attemptId'], 'atMonotonicNs': COMMON.ns()})
    try:
        selected_files = {}
        for name, path in [('docker', plan['runtimeSettings']['dockerExecutable']),
                           ('host-python', plan['runtimeSettings']['hostPython']),
                           ('host-node', Path(plan['runtimeSettings']['nodeHome']) / 'bin/node')]:
            actual = file_identity(path, budget, True)
            expected = (runtime_binding(root / 'runtime.json')[0]['identities']['dockerExecutable'] if name == 'docker'
                        else plan['binding']['selected'][name])
            need((actual['bytes'], actual['sha256']) == (expected['bytes'], expected['sha256']),
                 'PROVENANCE_SELECTED_FILE_CHANGED')
            selected_files[name] = actual
        save(root / 'selected-files-before.json', selected_files)
        image, _ = docker.call(['image', 'inspect', plan['binding']['imageId']], inspect_kind='image')
        need(image['Id'] == plan['binding']['imageId'] and image['Os'] == 'linux' and
             not any(image['injectionEnvironmentPresent'].values()), 'PROVENANCE_IMAGE_ID_OR_INJECTION')
        save(root / 'image.json', image)
        records['host-python'] = collect_role(root, plan, 'host-python', budget, docker)
        records['host-node'] = collect_role(root, plan, 'host-node', budget, docker)
        cid = docker.create()
        records['driver-python'] = collect_role(root, plan, 'driver-python', budget, docker, cid)
        for role in ROLES:
            validate_role(records[role], plan, root, role)
        final_files = {name: file_identity(value['path'], budget, True) for name, value in selected_files.items()}
        need(final_files == selected_files, 'PROVENANCE_SELECTED_FILE_CHANGED')
        save(root / 'selected-files-after.json', final_files)
    except BaseException as error:
        failure = str(error)[:256] if isinstance(error, COMMON.WorkflowError) else type(error).__name__
    finally:
        budget.cleanup_deadline = time.monotonic() + spec['budgets']['cleanupMs'] / 1000
        try:
            cleanup = docker.cleanup()
        except BaseException as error:
            cleanup = {'containerRemoved': False, 'error': str(error)[:256] if isinstance(error, COMMON.WorkflowError)
                       else type(error).__name__}
        result = {'schemaVersion': 1, 'kind': 'http-runtime-provenance-result', 'attemptId': plan['attemptId'],
            'status': 'completed' if failure is None and cleanup['containerRemoved'] else 'failed',
            'failure': failure, 'cleanup': cleanup, 'roles': {role: records.get(role, {}).get('status', 'missing')
                for role in ROLES}, 'rawBytes': budget.raw_bytes, 'hashBytes': budget.hash_bytes,
            'startedMonotonicNs': str(int(budget.started * 1e9)), 'finishedMonotonicNs': COMMON.ns(),
            'clockAlignment': 'unverified', 'performanceAccepted': False}
        save(root / 'result.json', result)
        files = [COMMON.file_entry(path, path.relative_to(root).as_posix()) for path in COMMON.regular_files(root)]
        need(sum(row['bytes'] for row in files) + 65536 <= spec['budgets']['maxEvidenceBytes'],
             'PROVENANCE_FINAL_EVIDENCE_BUDGET')
        save(root / 'raw.lock.json', {'schemaVersion': 1, 'kind': 'http-runtime-provenance-raw-lock',
            'files': files, 'contentDigest': value_hash(files)})
    verified = verify(root)
    if verified['valid']:
        save(root / 'runtime-binding.json', verified['runtimeBinding'])
        save(root / 'external-expectations.json', verified['externalExpectations'])
    return {'proof': str(root), **verified}


def stat_ticks(raw, pid):
    need(type(raw) is str and len(raw.encode()) <= 4096 and raw.startswith(str(pid) + ' ('), 'PROVENANCE_STAT_PID')
    tail = raw.rsplit(')', 1)[-1].split()
    need(len(tail) >= 20 and tail[0] not in ('Z', 'X', 'x') and re.fullmatch(r'[0-9]+', tail[19]),
         'PROVENANCE_STAT_FIELDS')
    return tail[19]


def offsets(raw):
    need(type(raw) is str and len(raw.encode()) <= 256, 'PROVENANCE_OFFSETS')
    rows = [row.split() for row in raw.strip().splitlines()]
    need(len(rows) == 2 and [row[0] for row in rows] == ['monotonic', 'boottime'] and
         all(len(row) == 3 and re.fullmatch(r'-?[0-9]{1,12}', row[1]) and re.fullmatch(r'[0-9]{1,9}', row[2]) and
             int(row[2]) < 10**9 for row in rows), 'PROVENANCE_OFFSETS')
    return {row[0]: [str(int(row[1])), str(int(row[2]))] for row in rows}


def validate_role(record, plan, root, role):
    need(record.get('role') == role and record.get('status') == 'completed' and
         record.get('childExitCode') == 0 and record.get('stderrBytes') == 0 and len(record['frames']) == 2,
         'PROVENANCE_ROLE_INCOMPLETE')
    need(all(type(raw) is str and len(raw.encode()) <= 32768 and raw.endswith('\n') and raw.count('\n') == 1
             for raw in record['frames']), 'PROVENANCE_ROLE_FRAME_BUDGET')
    frames = [json.loads(raw, object_pairs_hook=COMMON.unique) for raw in record['frames']]
    need(frames == [record['ready'], record['final']] and
         sum(len(raw.encode()) for raw in record['frames']) == record['stdoutBytes'], 'PROVENANCE_ROLE_RAW_BINDING')
    request = {'schemaVersion': 1, 'kind': 'http-runtime-observer-request', 'role': role, 'token': plan['attemptId'],
               'runtimeDigest': plan['binding']['runtimeDigest'],
               **{key: plan['spec']['budgets'][key] for key in ('wallMs', 'handshakeMs', 'maxExecutableBytes')}}
    identities, proc = [], []
    for kind, frame, parent in zip(('ready', 'final'), frames, (record['before'], record['after'])):
        exact(frame, ('kind', 'request', 'identity', 'executable'), 'PROVENANCE_OBSERVER_FIELDS')
        need(frame['kind'] == kind and frame['request'] == request, 'PROVENANCE_REQUEST_BINDING')
        identity = frame['identity']
        exact(identity, ('pid', 'parentPid', 'statRaw', 'startTimeTicks', 'bootId', 'namespaces', 'timeOffsetsRaw',
                         'clock', 'runtime'), 'PROVENANCE_IDENTITY_FIELDS')
        need(type(parent['pid']) is int and parent['pid'] > 0 and type(identity['pid']) is int and identity['pid'] > 0,
             'PROVENANCE_PID')
        ticks = stat_ticks(identity['statRaw'], identity['pid'])
        need(ticks == identity['startTimeTicks'] == stat_ticks(parent['statBeforeRaw'], parent['pid']) ==
             stat_ticks(parent['statAfterRaw'], parent['pid']), 'PROVENANCE_PID_START_BINDING')
        need(re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', identity['bootId']) and
             parent['bootId'] == identity['bootId'] and parent['bootIdRaw'].strip() == parent['bootId'] and
             len(parent['bootIdRaw'].encode()) <= 128, 'PROVENANCE_BOOT_BINDING')
        exact(identity['namespaces'], NS, 'PROVENANCE_NAMESPACE_FIELDS')
        for name in NS:
            prefix = 'time' if name == 'time_for_children' else name
            need(re.fullmatch(prefix + r':\[[0-9]+\]', identity['namespaces'][name]), 'PROVENANCE_NAMESPACE')
        need(identity['namespaces'] == parent['namespaces'] and
             offsets(identity['timeOffsetsRaw']) == offsets(parent['timeOffsetsRaw']), 'PROVENANCE_NAMESPACE_BINDING')
        status = dict(row.split(':', 1) for row in parent['statusRaw'].splitlines() if ':' in row)
        need(len(parent['statusRaw'].encode()) <= 16384, 'PROVENANCE_STATUS_BUDGET')
        nspid = status.get('NSpid', '').split()
        need(nspid and all(re.fullmatch(r'[0-9]+', value) for value in nspid) and
             int(nspid[0]) == parent['pid'] and int(nspid[-1]) == identity['pid'], 'PROVENANCE_NSPID_BINDING')
        need(status.get('Uid', '').split() == [str(plan['owner']['uid'])] * 4, 'PROVENANCE_PROCESS_UID')
        cmdline = base64.b64decode(parent['cmdlineBase64'], validate=True).split(b'\0')
        need(sum(len(value) + 1 for value in cmdline[:-1]) <= 8192, 'PROVENANCE_CMDLINE_BUDGET')
        need(cmdline[-1] == b'' and [value.decode() for value in cmdline[:-1]] ==
             worker_argv(plan, Path(plan['root']), role), 'PROVENANCE_PROCESS_ARGV')
        runtime = identity['runtime']
        need(type(runtime.get('injectionEnvironmentPresent')) is dict and
             set(runtime['injectionEnvironmentPresent']) == set(FLAGS) and
             all(value is False for value in runtime['injectionEnvironmentPresent'].values()),
             'PROVENANCE_RUNTIME_INJECTION')
        need(type(runtime.get('reportedExecutable')) is str and runtime['reportedExecutable'].startswith('/'),
             'PROVENANCE_RUNTIME_PATH')
        if role == 'host-node':
            need(runtime.get('name') == 'node' and re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', runtime['version']) and
                 re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', runtime['uv']) and runtime['platform'] == 'linux' and
                 re.fullmatch(SHA, runtime['execArgvSha256']) and identity['clock']['api'] == 'process.hrtime.bigint' and
                 re.fullmatch(r'[0-9]+', identity['clock']['nowNs']), 'PROVENANCE_NODE_RUNTIME')
        else:
            need(runtime.get('name') == 'python' and runtime.get('implementation') == 'cpython' and
                 re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', runtime['version']) and
                 type(runtime['versionText']) is str and len(runtime['versionText']) <= 2048 and
                 runtime['versionText'].split()[0] == runtime['version'] and
                 runtime['flags'] == {'isolated': 1, 'ignore_environment': 1, 'no_site': 1, 'no_user_site': 1,
                                      'dont_write_bytecode': 1} and
                 identity['clock']['api'] == 'time.monotonic_ns' and identity['clock']['monotonic'] is True and
                 identity['clock']['adjustable'] is False and
                 type(identity['clock']['implementation']) is str and len(identity['clock']['implementation']) <= 256 and
                 type(identity['clock']['resolutionSeconds']) in (int, float) and
                 math.isfinite(identity['clock']['resolutionSeconds']) and identity['clock']['resolutionSeconds'] > 0,
                 'PROVENANCE_PYTHON_RUNTIME')
        executable = frame['executable']
        keys = ('bytes', 'device', 'inode', 'mtimeNs', 'ctimeNs')
        exact(executable, keys + (('sha256',) if kind == 'ready' else ()), 'PROVENANCE_EXECUTABLE_FIELDS')
        need(type(executable['bytes']) is int and 0 < executable['bytes'] <= plan['spec']['budgets']['maxExecutableBytes'] and
             all(re.fullmatch(r'[0-9]+', executable[key]) for key in keys if key != 'bytes'),
             'PROVENANCE_EXECUTABLE_METADATA')
        need(all(executable[key] == parent['executable'][key] for key in keys), 'PROVENANCE_EXECUTABLE_PROC_BINDING')
        need(parent['executable']['path'] == '/proc/' + str(parent['pid']) + '/exe' and
             re.fullmatch(SHA, parent['executable']['sha256']), 'PROVENANCE_EXECUTABLE_SOURCE')
        identities.append({key: identity[key] for key in identity if key not in ('statRaw', 'clock')})
        proc.append(parent['executable'])
    need(identities[0] == identities[1] and record['before']['pid'] == record['after']['pid'] and
         int(record['before']['observedMonotonicNs']) <= int(record['after']['observedMonotonicNs']) and
         all(proc[0][key] == proc[1][key] for key in ('bytes', 'sha256', 'device', 'inode', 'mtimeNs', 'ctimeNs')) and
         frames[0]['executable']['sha256'] == proc[0]['sha256'], 'PROVENANCE_RUNTIME_DRIFT')
    expected_source = 'docker-inspect-State.Pid' if role == 'driver-python' else 'owned-Popen.pid'
    need(record['actualPidSource'] == expected_source, 'PROVENANCE_PID_SOURCE')
    if role != 'driver-python':
        expected = plan['binding']['selected'][role]
        need(proc[0]['bytes'] == expected['bytes'] and proc[0]['sha256'] == expected['sha256'] and
             record['argv'] == worker_argv(plan, Path(plan['root']), role), 'PROVENANCE_LOCKED_EXECUTABLE_BINDING')
    else:
        live = read_json(root / 'container-live.json')
        verify_owned(live, plan, COMMON.file_entry(root / 'plan.json')['sha256'])
        verify_limits(live, plan, Path(plan['root']))
        need(live['State']['Running'] is True and live['State']['Pid'] == record['before']['pid'] and
             frames[0]['identity']['pid'] == 1 and record['argv'] == [plan['runtimeSettings']['dockerExecutable'],
             '--host', 'unix:///var/run/docker.sock', 'start', '--attach', '--interactive', live['Id']],
             'PROVENANCE_DRIVER_PID_SOURCE')
    return {'executableSha256': proc[0]['sha256'], 'bytes': proc[0]['bytes'], 'runtime': frames[0]['identity']['runtime'],
            'identity': identities[0], 'raw': 'roles/' + role + '.json', 'source': expected_source}


def verify(proof):
    root, plan = load_proof(proof)
    lock = read_json(root / 'raw.lock.json')
    need(lock.get('schemaVersion') == 1 and lock.get('kind') == 'http-runtime-provenance-raw-lock' and
         type(lock.get('files')) is list and value_hash(lock['files']) == lock.get('contentDigest'),
         'PROVENANCE_RAW_LOCK')
    excluded = {'raw.lock.json', 'runtime-binding.json', 'external-expectations.json'}
    paths = [path for path in COMMON.regular_files(root) if path.relative_to(root).as_posix() not in excluded and
             not path.relative_to(root).as_posix().startswith('recovery/')]
    actual = [COMMON.file_entry(path, path.relative_to(root).as_posix()) for path in paths]
    fixed = {'spec.json', 'runtime.json', 'runtime.lock.json', 'plan.json', 'plan.lock.json', 'dispatch.json',
             'result.json', 'image.json', 'container-created.json', 'container-live.json', 'container.id', 'stop-request.json',
             'selected-files-before.json', 'selected-files-after.json'}
    allowed = fixed | {'control/' + name for name in SOURCES} | {
        'control/' + role + '.json' for role in ROLES} | {'roles/' + role + '.json' for role in ROLES}
    need(all(row['path'] in allowed or re.fullmatch(r'docker/command-[0-9]{3}\.json', row['path'])
             for row in actual), 'PROVENANCE_UNKNOWN_EVIDENCE_FILE')
    need(actual == lock['files'] and sum(row['bytes'] for row in actual) + (root / 'raw.lock.json').stat().st_size <=
         plan['spec']['budgets']['maxEvidenceBytes'], 'PROVENANCE_RAW_CONTENT_CHANGED')
    for name in SOURCES:
        need(COMMON.file_entry(HERE / name)['sha256'] == next(item['sha256'] for item in plan['sources']
             if item['path'] == 'control/' + name), 'PROVENANCE_UNSUPPORTED_IMPLEMENTATION')
    result = read_json(root / 'result.json')
    need(result.get('schemaVersion') == 1 and result.get('kind') == 'http-runtime-provenance-result' and
         result.get('attemptId') == plan['attemptId'] and type(result['rawBytes']) is int and
         result['rawBytes'] >= 0 and type(result['hashBytes']) is int and result['hashBytes'] >= 0,
         'PROVENANCE_RESULT_BUDGET')
    base = {'schemaVersion': 1, 'kind': 'http-runtime-provenance-verification', 'proofDigest': lock['contentDigest'],
            'runtimeDigest': plan['binding']['runtimeDigest'], 'clockAlignment': 'unverified',
            'futureExperimentsOnly': True, 'performanceAccepted': False}
    if result['status'] != 'completed':
        return {**base, 'valid': False, 'expectationsAvailable': False, 'issues': ['probe-failed-or-cleanup-unproven'],
                'result': result}
    need(result['failure'] is None and result['cleanup'].get('containerRemoved') is True and
         result['roles'] == {role: 'completed' for role in ROLES} and
         result['rawBytes'] <= plan['spec']['budgets']['maxRawBytes'] and
         result['hashBytes'] <= plan['spec']['budgets']['maxHashBytes'], 'PROVENANCE_RESULT_INCOMPLETE')
    image, created, live = (read_json(root / name) for name in ('image.json', 'container-created.json', 'container-live.json'))
    need(image['Id'] == plan['binding']['imageId'] and image['Os'] == 'linux' and
         not any(image['injectionEnvironmentPresent'].values()), 'PROVENANCE_IMAGE_BINDING')
    for info in (created, live):
        verify_owned(info, plan, COMMON.file_entry(root / 'plan.json')['sha256'])
        verify_limits(info, plan, Path(plan['root']))
    need(created['Id'] == live['Id'] == result['cleanup'].get('containerId') and
         read_bytes(root / 'container.id', 65).decode().strip() == live['Id'], 'PROVENANCE_CONTAINER_CHAIN')
    commands = [read_json(path) for path in sorted((root / 'docker').glob('command-*.json'))]
    prefix = [plan['runtimeSettings']['dockerExecutable'], '--host', 'unix:///var/run/docker.sock']
    builder = Docker(root, plan, Budget(plan['spec']))
    builder.root = Path(plan['root'])
    expected_create = builder.create_args()
    need(commands and all(command['argv'][:3] == prefix and command['error'] is None for command in commands) and
         any(command['argv'][3:] == ['image', 'inspect', plan['binding']['imageId']] and
             command.get('inspection') == image and command['exitCode'] == 0 for command in commands) and
         any(command['argv'][3:] == expected_create and command['exitCode'] == 0 for command in commands) and
         any(command.get('inspection') == created and command['exitCode'] == 0 for command in commands) and
         any(command.get('inspection') == live and command['exitCode'] == 0 for command in commands) and
         any(command.get('inspection', {}).get('Id') == live['Id'] and
             command['inspection']['State']['Running'] is False and
             command['inspection']['State']['ExitCode'] == 0 and
             command['inspection']['State']['OOMKilled'] is False for command in commands) and
         any(command['argv'][3:] == ['rm', '-f', live['Id']] and command['exitCode'] == 0 for command in commands) and
         commands[-1]['argv'][3:] == ['inspect', '--type', 'container', live['Id']] and
         commands[-1]['notFound'] is True, 'PROVENANCE_DOCKER_RAW_CHAIN')
    observed = {role: validate_role(read_json(root / 'roles' / (role + '.json')), plan, root, role) for role in ROLES}
    selected = read_json(root / 'selected-files-before.json')
    need(selected == read_json(root / 'selected-files-after.json') and set(selected) == {'docker', 'host-python', 'host-node'},
         'PROVENANCE_SELECTED_FILE_DRIFT')
    expected_docker = runtime_binding(root / 'runtime.json')[0]['identities']['dockerExecutable']
    for name in selected:
        expected = expected_docker if name == 'docker' else plan['binding']['selected'][name]
        need((selected[name]['bytes'], selected[name]['sha256']) == (expected['bytes'], expected['sha256']),
             'PROVENANCE_SELECTED_FILE_BINDING')
    raw_count = sum(command['stdoutBytes'] + command['stderrBytes'] for command in commands)
    for role in ROLES:
        record = read_json(root / 'roles' / (role + '.json'))
        raw_count += record['stdoutBytes'] + record['stderrBytes']
        for parent in (record['before'], record['after']):
            raw_count += sum(len(parent[key].encode()) for key in
                ('statBeforeRaw', 'statAfterRaw', 'timeOffsetsRaw', 'statusRaw', 'bootIdRaw'))
            raw_count += len(base64.b64decode(parent['cmdlineBase64'], validate=True))
    hash_count = 2 * sum(value['bytes'] for value in selected.values()) + 3 * sum(row['bytes'] for row in observed.values())
    need(result['rawBytes'] == raw_count and result['hashBytes'] == hash_count, 'PROVENANCE_RAW_HASH_ACCOUNTING')
    driver, node, host = observed['driver-python'], observed['host-node'], observed['host-python']
    binding = {'schemaVersion': 1, 'kind': 'http-runtime-binding', 'sourceOrigin': 'linux-procfs',
        'runtimeDigest': plan['binding']['runtimeDigest'], 'driver': {'imageId': image['Id'],
            'pythonExecutableSha256': driver['executableSha256'], 'pythonVersion': driver['runtime']['version'],
            'pythonImplementation': driver['runtime']['implementation']},
        'node': {'executableSha256': node['executableSha256'], 'version': node['runtime']['version'],
                 'uv': node['runtime']['uv']}, 'proofDigest': lock['contentDigest']}
    expectations = {'schemaVersion': 1, 'kind': 'http-runtime-provenance-expectations', 'sourceOrigin': 'linux-procfs',
        'probedRuntimeDigest': binding['runtimeDigest'], 'proofDigest': binding['proofDigest'], 'runtimeBinding': binding,
        'hostPythonExecutableSha256': host['executableSha256'], 'hostPythonVersion': host['runtime']['version'],
        'hostPythonImplementation': host['runtime']['implementation'],
        'actualRuntimeBinding': {'dockerImageId': image['Id'], 'containerId': live['Id'], 'roles': observed,
            'sourceFiles': plan['sources'], 'runtimeFileSha256': plan['binding']['runtimeFileSha256']},
        'usage': 'freeze selected facts before a new experiment; probedRuntimeDigest is an audit reference, not a future runtimeDigest requirement',
        'clockAlignment': 'unverified', 'performanceAccepted': False}
    for name, derived in [('runtime-binding.json', binding), ('external-expectations.json', expectations)]:
        if (root / name).exists():
            need(read_json(root / name) == derived, 'PROVENANCE_DERIVED_OUTPUT_CHANGED')
    return {**base, 'valid': True, 'expectationsAvailable': True, 'runtimeBinding': binding,
            'externalExpectations': expectations, 'issues': []}


def stop(proof):
    root, plan = load_proof(proof)
    need(sys.platform == 'linux' and os.geteuid() != 0 and
         plan['owner'] == {'uid': os.geteuid(), 'gid': os.getegid()} and plan['root'] == str(root),
         'PROVENANCE_STOP_OWNER_OR_HOST')
    if not (root / 'result.json').exists():
        marker = {'attemptId': plan['attemptId'], 'planSha256': COMMON.file_entry(root / 'plan.json')['sha256']}
        try:
            save(root / 'stop-request.json', marker)
        except FileExistsError:
            pass
        need(read_json(root / 'stop-request.json') == marker, 'PROVENANCE_STOP_IDENTITY')
    recovery = root / 'recovery' / uuid.uuid4().hex
    recovery.mkdir(parents=True)
    budget = Budget(plan['spec'])
    budget.cleanup_deadline = time.monotonic() + plan['spec']['budgets']['cleanupMs'] / 1000
    docker = Docker(root, plan, budget)
    docker.record_root = recovery
    try:
        result = docker.cleanup()
    except Exception as error:
        result = {'containerRemoved': False, 'error': str(error)[:256] if isinstance(error, COMMON.WorkflowError)
                  else type(error).__name__}
    save(recovery / 'result.json', result)
    return {'proof': str(root), 'cleanup': result, 'recovery': str(recovery),
            'expectationsAvailable': False, 'performanceAccepted': False}


def describe():
    return {'schemaVersion': 1, 'kind': 'http-runtime-provenance-capability',
        'commands': {'probe': ['--spec', '--runtime', '--out'], 'verify': ['--proof'], 'stop': ['--proof']},
        'spec': 'http-runtime-provenance-spec/v1; explicit budgets and container limits, no defaults',
        'requiredSpecFields': ['schemaVersion', 'kind', 'sourceOrigin', 'budgets', 'container'],
        'budgetFields': ['wallMs', 'commandMs', 'handshakeMs', 'cleanupMs', 'maxRawBytes', 'maxEvidenceBytes',
                         'maxExecutableBytes', 'maxHashBytes'],
        'containerFields': ['cpus', 'memoryBytes', 'pids', 'tmpfsBytes'],
        'fixedSources': list(SOURCES),
        'effects': {'probe': 'three fixed observers, one isolated owned Docker probe; no HTTP or target mutations',
                    'verify': 'offline raw/source/runtime/identity reconstruction',
                    'stop': 'precise owned probe cleanup only; does not qualify an incomplete proof'},
        'sourceOrigin': 'linux-procfs', 'dockerSocket': 'unix:///var/run/docker.sock',
        'externalExpectationsUsage': 'future predeclared experiments only',
        'clockAlignment': 'unverified', 'performanceAccepted': False}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest='action', required=True)
    commands.add_parser('describe')
    create = commands.add_parser('probe', help='Explicit short Linux/Docker probe; creates one owned isolated container')
    for key in ('spec', 'runtime', 'out'):
        create.add_argument('--' + key, type=Path, required=True)
    for name in ('verify', 'stop'):
        commands.add_parser(name).add_argument('--proof', type=Path, required=True)
    args = cli.parse_args()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, cancel)
    try:
        if args.action == 'probe':
            result = probe(args.spec, args.runtime, args.out)
            ok = result['valid']
        else:
            result = {'describe': describe, 'verify': lambda: verify(args.proof), 'stop': lambda: stop(args.proof)}[args.action]()
            ok = True
        reply = {'ok': ok, 'data': result}
        if not ok:
            reply['error'] = {'code': 'PROVENANCE_INCOMPLETE', 'proof': result['proof'], 'issues': result['issues']}
        print(json.dumps(reply, allow_nan=False))
        return 0 if ok else 6
    except BaseException as error:
        print(json.dumps({'ok': False, 'error': {'code': str(error)[:256] if isinstance(error, COMMON.WorkflowError)
              else type(error).__name__, 'retryable': False}}))
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
