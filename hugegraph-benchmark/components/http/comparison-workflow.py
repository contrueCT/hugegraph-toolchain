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

"""Freeze/run/status/stop one local Linux Docker paired HTTP comparison.

Uses the public HG-Bench environment, task and HTTP CLI. Run is single-dispatch;
a lost connection permits status/stop, never another run. Independent target and
driver container watchdogs bound orphaned work. No SSH, image pull, graph import,
retry, formal performance acceptance, or arbitrary command hooks.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
import importlib.util

sys.dont_write_bytecode = True
MiB = 1024 ** 2
LABEL = 'org.apache.hugegraph.hgbench.comparison'
SOURCE_TREES = ('components', 'dist', 'src', 'benchmarks', 'node_modules')
SOURCE_FILES = ('package.json', 'release/dependency-lock.json')
CANCELLED = False


def go_extension():
    spec = importlib.util.spec_from_file_location('hgbench_go_comparison_runtime',
                                                Path(__file__).with_name('go-comparison-runtime.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_selection(runtime):
    return SOURCE_TREES + SOURCE_FILES + (('release/http-go-toolchain.json',)
                                        if runtime.get('schemaVersion') == 2 else ())


def engine_control_files(runtime):
    return go_extension().CONTROL_FILES if runtime.get('schemaVersion') == 2 else ()


class WorkflowError(Exception):
    pass


def need(value, code):
    if not value:
        raise WorkflowError(code)


def error_message(error):
    if isinstance(error, WorkflowError) or type(error).__name__ == 'GoRuntimeError' or str(error).startswith('GO_PROCESS:'):
        return str(error)
    return type(error).__name__


def signal_cancel(_signal, _frame):
    global CANCELLED
    CANCELLED = True  # No locks or I/O in a signal handler.


def ns():
    return str(time.monotonic_ns())


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2, allow_nan=False) + '\n').encode()


def value_hash(value):
    # Execution locks contain only ASCII paths and integer sizes; matches paired/core.valueHash.
    return sha(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode())


def read_bytes(path, cap=16 * MiB):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode) and before.st_size <= cap, 'input-file-budget-or-type')
        raw = stream.read(cap + 1)
        after = os.fstat(stream.fileno())
    need(len(raw) == before.st_size and before.st_size == after.st_size and
         before.st_mtime_ns == after.st_mtime_ns, 'input-changed-or-too-large')
    return raw


def unique(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, 'duplicate-json-key')
        result[key] = value
    return result


def read_json(path, cap=16 * MiB):
    return json.loads(read_bytes(path, cap), object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(WorkflowError('nonfinite-json')))


def verify_dependency_lock(root):
    root = Path(root)
    package = read_json(root / 'package.json', 8 * MiB)
    binding = package.get('hgbenchRelease', {}).get('dependencyLock', {})
    need(isinstance(binding, dict) and set(binding) == {'path', 'bytes', 'sha256'} and
         binding['path'] == 'release/dependency-lock.json' and type(binding['bytes']) is int and
         0 < binding['bytes'] <= 8 * MiB and isinstance(binding['sha256'], str) and
         re.fullmatch('[a-f0-9]{64}', binding['sha256']), 'release-dependency-lock-binding')
    need(stat.S_ISDIR((root / 'release').lstat().st_mode), 'release-dependency-lock-directory')
    raw = read_bytes(root / binding['path'], 8 * MiB)
    need(len(raw) == binding['bytes'] and sha(raw) == binding['sha256'], 'release-dependency-lock-digest')
    lock = json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(WorkflowError('nonfinite-json')))
    need(lock.get('lockfileVersion') == 3 and lock.get('name') == package.get('name') and
         lock.get('version') == package.get('version'), 'release-dependency-lock-package')
    declaration = lock.get('packages', {}).get('')
    need(isinstance(declaration, dict) and all(declaration.get(key) == package.get(key) for key in
         ('name', 'version', 'license', 'bin', 'engines', 'dependencies', 'devDependencies')),
         'release-dependency-lock-declarations')
    return binding


def save(path, value, replace=False):
    path = Path(path)
    target = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp') if replace else path
    raw = encode(value)
    with target.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    if replace:
        os.replace(target, path)


def file_entry(path, relative=None, check=lambda: None):
    path = Path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode), 'source-special-file')
        digest = hashlib.sha256()
        while True:
            check()
            raw = stream.read(MiB)
            if not raw:
                break
            digest.update(raw)
        after = os.fstat(stream.fileno())
    need(before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns, 'source-changed')
    return {'path': relative or str(path), 'bytes': after.st_size, 'sha256': digest.hexdigest()}


def inventory(root, selected=None, check=lambda: None):
    root = Path(root).resolve(strict=True)
    result = []

    def visit(path):
        check()
        relative = path.relative_to(root).as_posix()
        need(len(relative) <= 2048 and len(result) < 100000, 'inventory-limit')
        info = path.lstat()
        row = {'path': relative, 'mode': stat.S_IMODE(info.st_mode)}
        if stat.S_ISLNK(info.st_mode):
            target = os.readlink(path)
            resolved = path.resolve(strict=True)
            need(not os.path.isabs(target) and resolved.is_relative_to(root), 'source-link-escapes-root')
            if selected is not None:
                need(any(resolved == root / name or resolved.is_relative_to(root / name) for name in selected),
                     'source-link-target-not-locked')
            row.update(type='symlink', target=target)
        elif stat.S_ISDIR(info.st_mode):
            row['type'] = 'directory'
        else:
            need(stat.S_ISREG(info.st_mode), 'source-special-file')
            row.update(type='file', **{k: v for k, v in file_entry(path, relative, check).items() if k != 'path'})
        result.append(row)
        if row['type'] == 'directory':
            for child in sorted(path.iterdir()):
                visit(child)

    for name in selected or sorted(p.name for p in root.iterdir()):
        visit(root / name)
    return sorted(result, key=lambda row: row['path'])


def regular_files(root):
    root = Path(root)
    result = []
    for folder, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(folder) / name
            mode = path.lstat().st_mode
            need(stat.S_ISDIR(mode) or stat.S_ISREG(mode), 'evidence-link-or-special-file')
            if stat.S_ISREG(mode):
                result.append(path)
            need(len(result) <= 100000, 'evidence-file-count')
    return sorted(result)


class ActiveUsage:
    """Bounded, no-follow sampling of a live directory; not a content inventory."""
    MAX_ENTRIES = 100000
    MAX_DEPTH = 64
    MAX_SECONDS = 5
    MAX_PATH_SAMPLES = 8
    MAX_PATH_LENGTH = 256

    def __init__(self, root, byte_budget, budget_code, min_free_bytes=None):
        self.root = Path(root)
        self.byte_budget = byte_budget
        self.budget_code = budget_code
        self.min_free_bytes = min_free_bytes
        self.identity = None
        self.audit = {'schemaVersion': 1, 'kind': 'http-active-directory-usage', 'samplingOnly': True,
                      'pathEncoding': 'unicode-escape',
                      'byteBudget': byte_budget, 'minFreeBytes': min_free_bytes, 'scans': 0,
                      'vanishedEntries': 0, 'vanishedPathSamples': [], 'peakObservedBytes': 0,
                      'first': None, 'latest': None,
                      'limits': {'maxEntries': self.MAX_ENTRIES, 'maxDepth': self.MAX_DEPTH,
                                 'maxSeconds': self.MAX_SECONDS, 'maxPathSamples': self.MAX_PATH_SAMPLES,
                                 'maxPathLength': self.MAX_PATH_LENGTH}}

    @staticmethod
    def identity_of(info):
        return {'device': str(info.st_dev), 'inode': str(info.st_ino)}

    def sample(self, check=lambda: None):
        started = time.monotonic()
        row = {'startedMonotonicNs': ns(), 'finishedMonotonicNs': None, 'observedBytes': 0,
               'entries': 0, 'files': 0, 'directories': 0, 'vanishedEntries': 0,
               'vanishedPathSamples': [], 'freeBytes': None, 'outcome': 'failed', 'failure': None}
        current_path = '.'

        def bounded():
            check()
            need(time.monotonic() - started < self.MAX_SECONDS, 'active-scan-deadline')

        def path_sample(relative):
            escaped = relative.encode('unicode_escape').decode('ascii')
            return {'path': escaped[:self.MAX_PATH_LENGTH], 'truncated': len(escaped) > self.MAX_PATH_LENGTH}

        def vanished(relative):
            row['vanishedEntries'] += 1
            sample = path_sample(relative)
            if len(row['vanishedPathSamples']) < self.MAX_PATH_SAMPLES:
                row['vanishedPathSamples'].append(sample)
            if len(self.audit['vanishedPathSamples']) < self.MAX_PATH_SAMPLES:
                self.audit['vanishedPathSamples'].append(sample)

        def visit(fd, relative, depth):
            nonlocal current_path
            bounded()
            need(depth <= self.MAX_DEPTH, 'active-scan-depth-limit')
            with os.scandir(fd) as entries:
                for entry in entries:
                    bounded()
                    child = relative + '/' + entry.name if relative else entry.name
                    current_path = child
                    row['entries'] += 1
                    need(row['entries'] <= self.MAX_ENTRIES, 'active-scan-entry-limit')
                    need(len(child) <= 2048, 'active-scan-path-limit')
                    try:
                        info = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
                    except FileNotFoundError:
                        vanished(child)  # Only children already returned by scandir may disappear.
                        continue
                    if stat.S_ISREG(info.st_mode):
                        row['files'] += 1
                        row['observedBytes'] += info.st_size
                        need(row['observedBytes'] <= self.byte_budget, self.budget_code)
                        continue
                    need(stat.S_ISDIR(info.st_mode), 'active-scan-link-or-special-file')
                    try:
                        child_fd = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                           dir_fd=fd)
                    except FileNotFoundError:
                        vanished(child)
                        continue
                    try:
                        need(self.identity_of(os.fstat(child_fd)) == self.identity_of(info),
                             'active-scan-directory-identity-changed')
                        row['directories'] += 1
                        visit(child_fd, child, depth + 1)
                        current_path = child
                        try:
                            after = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
                        except FileNotFoundError:
                            vanished(child)
                            continue
                        need(stat.S_ISDIR(after.st_mode) and self.identity_of(after) == self.identity_of(info),
                             'active-scan-directory-identity-changed')
                    finally:
                        os.close(child_fd)

        try:
            bounded()
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                before = os.fstat(fd)
                identity = self.identity_of(before)
                if self.identity is None:
                    self.identity = identity
                need(identity == self.identity, 'active-scan-root-identity-changed')
                row['rootIdentity'] = identity
                # Capacity is checked independently of sampled logical file bytes.
                storage = os.fstatvfs(fd)
                row['freeBytes'] = storage.f_bavail * storage.f_frsize
                need(self.min_free_bytes is None or row['freeBytes'] >= self.min_free_bytes, 'disk-reserve')
                visit(fd, '', 0)
                current_path = '.'
                after = self.root.lstat()
                need(stat.S_ISDIR(after.st_mode) and self.identity_of(after) == identity,
                     'active-scan-root-identity-changed')
                bounded()
            finally:
                os.close(fd)
            row['outcome'] = 'within-budget'
        except BaseException as error:
            row['failure'] = str(error) if isinstance(error, WorkflowError) else 'active-scan-io-error'
            row['failurePath'] = path_sample(current_path)
            if isinstance(error, OSError):
                row['errno'] = error.errno
                raise WorkflowError(row['failure']) from error
            raise
        finally:
            row['finishedMonotonicNs'] = ns()
            self.audit['scans'] += 1
            self.audit['vanishedEntries'] += row['vanishedEntries']
            self.audit['peakObservedBytes'] = max(self.audit['peakObservedBytes'], row['observedBytes'])
            if self.audit['first'] is None:
                self.audit['first'] = row
            self.audit['latest'] = row
        return row


def copy_regular_tree(source, destination, cap, check=lambda: None):
    source, destination = Path(source), Path(destination)
    check()
    need(not destination.exists(), 'archive-already-exists')
    files = regular_files(source)
    need(sum(p.stat().st_size for p in files) <= cap, 'archive-byte-budget')
    destination.mkdir()
    for path in files:
        check()
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream, target.open('xb') as output:
            before = os.fstat(stream.fileno())
            need(stat.S_ISREG(before.st_mode), 'archive-source-type')
            count = 0
            while True:
                check()
                raw = stream.read(65536)
                if not raw:
                    break
                count += len(raw)
                need(count <= before.st_size, 'archive-source-grew')
                output.write(raw)
            after = os.fstat(stream.fileno())
            need(count == before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns,
                 'archive-source-changed')


def exact(value, fields, code):
    need(isinstance(value, dict) and set(value) == set(fields), code)


def integer(value, low, high, code):
    need(type(value) is int and low <= value <= high, code)


def path_value(value):
    need(isinstance(value, str) and value.startswith('/') and len(value) <= 2048 and
         not any(ord(c) < 32 or c == ',' for c in value), 'absolute-clean-path-required')


def validate_runtime(value):
    go = value.get('schemaVersion') == 2
    exact(value, ('schemaVersion', 'kind', 'sourceRoot', 'nodeHome', 'hostPython', 'dockerExecutable',
                  'driver', 'collector', 'supervisor') + (('executor',) if go else ()), 'runtime-fields')
    need(value['schemaVersion'] in (1, 2) and value['kind'] == 'http-comparison-runtime', 'runtime-identity')
    if go:
        go_extension().validate(value['executor'])
    for name in ('sourceRoot', 'nodeHome', 'hostPython', 'dockerExecutable'):
        path_value(value[name])
    driver = value['driver']
    exact(driver, ('image', 'python', 'cpus', 'memoryBytes', 'pids', 'tmpfsBytes', 'wallSeconds',
                   'shutdownSeconds', 'maxLogBytes'), 'driver-fields')
    need(isinstance(driver['image'], str) and re.fullmatch(r'sha256:[a-f0-9]{64}', driver['image']), 'driver-image')
    path_value(driver['python'])
    need(type(driver['cpus']) in (int, float) and math.isfinite(driver['cpus']) and
         0.1 <= driver['cpus'] <= 64, 'driver-cpus')
    for name, low, high in [('memoryBytes', 128 * MiB, 128 * 1024 * MiB), ('pids', 32, 4096),
                             ('tmpfsBytes', 4 * MiB, 1024 * MiB), ('wallSeconds', 5, 86400),
                             ('shutdownSeconds', 1, 30), ('maxLogBytes', 4096, 16 * MiB)]:
        integer(driver[name], low, high, 'driver-' + name)
    need(driver['tmpfsBytes'] < driver['memoryBytes'], 'driver-tmpfs-memory-budget')
    collector = value['collector']
    exact(collector, ('intervalMs', 'durationMs', 'maxSamples', 'maxOutputBytes'), 'collector-fields')
    for name, low, high in [('intervalMs', 100, 60000), ('durationMs', 100, 86400000),
                             ('maxSamples', 1, 100000), ('maxOutputBytes', 131072, 64 * MiB)]:
        integer(collector[name], low, high, 'collector-' + name)
    need(collector['maxSamples'] >= math.ceil(collector['durationMs'] / collector['intervalMs']) + 1,
         'collector-sample-budget')
    supervisor = value['supervisor']
    exact(supervisor, ('pollMs', 'commandMs', 'maxCommandBytes', 'minFreeBytes', 'maxWorkBytes'), 'supervisor-fields')
    for name, low, high in [('pollMs', 100, 5000), ('commandMs', 1000, 300000),
                             ('maxCommandBytes', 4096, 16 * MiB), ('minFreeBytes', 0, 1024 ** 5),
                             ('maxWorkBytes', MiB, 1024 ** 4)]:
        integer(supervisor[name], low, high, 'supervisor-' + name)


def clean_env(python, node_home=None):
    return {'PATH': (str(node_home) + '/bin:' if node_home else '') + '/usr/local/bin:/usr/bin:/bin',
            'HGBENCH_PYTHON': str(python), 'PYTHONNOUSERSITE': '1', 'PYTHONDONTWRITEBYTECODE': '1',
            'LC_ALL': 'C', 'LANG': 'C'}


def jvm_evidence_reserve(settings, budgets, analysis_ms=0):
    observation = settings.get('jvmObservation')
    if observation is None:
        return 0
    raw, parser = observation['maxRawBytes'], observation['parser']
    integer(raw, 4096, 256 * MiB, 'jvm-raw-budget')
    integer(parser['maxOutputBytes'], raw + 2 * MiB, 512 * MiB, 'jvm-output-budget')
    integer(parser['timeoutMs'], 100, 300000, 'jvm-parser-deadline')
    need(raw == settings['budget']['maxLogBytes'], 'jvm-shared-log-cap')
    need(parser['timeoutMs'] + 1000 < budgets['sealAndAnalyzeMs'], 'jvm-parser-seal-budget')
    reparse_ms = budgets.get('armCount', 1) * (parser['timeoutMs'] + 5000)
    need(reparse_ms + analysis_ms + 60000 <= budgets['sealAndAnalyzeMs'], 'jvm-reverification-seal-budget')
    # Original closed stdout, a separately parsed raw copy, expanded events and receipts.
    return raw + parser['maxOutputBytes'] + 2 * MiB


def kill_group(process):
    # A reaped leader may leave a descendant with inherited stdout; still kill its owned group.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass


def command(argv, timeout, cap, env, guard=lambda: None, record=None, check=True,
            terminate_grace=0, terminate_deadline=None):
    guard()
    begun = ns()
    process = subprocess.Popen(list(map(str, argv)), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               env=env, start_new_session=True)
    selector = selectors.DefaultSelector()
    output = {'stdout': bytearray(), 'stderr': bytearray()}
    error = None
    deadline = time.monotonic() + timeout
    try:
        for name in output:
            stream = getattr(process, name)
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map() or process.poll() is None:
            guard()
            need(time.monotonic() < deadline, 'command-deadline-exceeded')
            for key, _ in selector.select(min(0.05, max(0, deadline - time.monotonic()))):
                raw = os.read(key.fd, 65536)
                if not raw:
                    selector.unregister(key.fileobj)
                else:
                    remaining = cap - sum(len(part) for part in output.values())
                    output[key.data].extend(raw[:max(0, remaining)])
                    need(len(raw) <= remaining, 'command-output-budget')
        process.wait(timeout=1)
    except BaseException as failure:
        error = str(failure) if isinstance(failure, WorkflowError) else type(failure).__name__
    finally:
        if error is not None and terminate_grace > 0:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except OSError:
                # A just-exited group can already be absent; the exact leader is still reaped below.
                pass
            closing = min(time.monotonic() + terminate_grace,
                          terminate_deadline if terminate_deadline is not None else math.inf)
            while time.monotonic() < closing and (selector.get_map() or process.poll() is None):
                for key, _ in selector.select(min(0.05, max(0, closing - time.monotonic()))):
                    raw = os.read(key.fd, 65536)
                    if not raw:
                        selector.unregister(key.fileobj)
                    else:
                        remaining = cap - sum(len(part) for part in output.values())
                        output[key.data].extend(raw[:max(0, remaining)])
        if terminate_grace > 0:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                pass
            remaining = 1 if terminate_deadline is None else max(0, min(1, terminate_deadline - time.monotonic()))
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                error = error or 'command-reap-deadline'
        else:
            kill_group(process)
        selector.close()
        process.stdout.close()
        process.stderr.close()
    value = {'argv': list(map(str, argv)), 'exitCode': process.returncode, 'error': error,
             'startedMonotonicNs': begun, 'finishedMonotonicNs': ns(),
             **{name: raw.decode('utf-8', errors='replace') for name, raw in output.items()}}
    if record:
        save(record, value)
    need(error is None, error)
    if check:
        need(value['exitCode'] == 0, 'command-failed-see-stdout-and-stderr')
    return value


def response(value):
    reply = json.loads(value['stdout'])
    need(value['exitCode'] == 0 and reply.get('ok') is True and isinstance(reply.get('data'), dict),
         'cli-structured-failure')
    return reply['data']


def freeze(runtime_file, comparison, output):
    deadline = time.monotonic() + 300
    def check():
        need(not CANCELLED and time.monotonic() < deadline, 'freeze-cancelled-or-deadline')
    runtime = read_json(runtime_file, 65536)
    validate_runtime(runtime)
    for key in ('sourceRoot', 'nodeHome', 'hostPython', 'dockerExecutable'):
        runtime[key] = str(Path(runtime[key]).resolve(strict=True))
    root = Path(output).parent.resolve(strict=True) / Path(output).name
    need(root.parent.is_dir() and not root.exists(), 'new-attempt-required')
    source, node = Path(runtime['sourceRoot']), Path(runtime['nodeHome'])
    verify_dependency_lock(source)
    need(not any(root.is_relative_to(source / name) for name in SOURCE_TREES) and
         not root.is_relative_to(node), 'attempt-in-runtime-tree')
    need(Path(__file__).resolve() == source / 'components/http/comparison-workflow.py', 'component-source-root-differs')
    need(Path(runtime['dockerExecutable']).name == 'docker', 'docker-executable-basename-required')
    env = clean_env(runtime['hostPython'], node)
    verification = response(command([node / 'bin/node', source / 'dist/cli.js', 'http', 'comparison', 'verify',
                                     Path(comparison).resolve(strict=True)], 300, 16 * MiB, env, check))
    plan = read_json(Path(comparison) / 'plan.json')
    need(verification['planDigest'] == plan['planDigest'] and plan['purpose'] in ('functional', 'calibration'),
         'comparison-not-executable')
    shared = read_json(Path(comparison) / 'inputs/settings.json')
    first_arm = plan['blocks'][0]['arms'][0]['arm']
    profile = read_json(Path(comparison) / ('profiles/' + first_arm + '.json'))
    go = runtime['schemaVersion'] == 2
    need(go == (profile.get('schemaVersion') == 2), 'profile-runtime-engine-mismatch')
    if go:
        runtime['executor']['binary']['path'] = str(Path(runtime['executor']['binary']['path']).resolve(strict=True))
        runtime['executor']['clockProof'] = str(Path(runtime['executor']['clockProof']).resolve(strict=True))
        go_extension().bind_profile(runtime, profile)
        proof = response(command([node / 'bin/node', source / 'dist/cli.js', 'http', 'engine', 'clock-verify',
                                  runtime['executor']['clockProof']], 30, MiB, env, check))
        expected = {k: v for k, v in runtime['executor']['binary'].items() if k != 'path'}
        need(proof['binary'] == expected and proof['imageId'] == runtime['driver']['image'] and
             proof['hostBootId'] == Path('/proc/sys/kernel/random/boot_id').read_text().strip() and
             proof['hostPythonExecutableSha256'] == file_entry(runtime['hostPython'], check=check)['sha256'],
             'Go-clock-proof-executor-host-mismatch')
    need(profile['endpoint'] == 'http://127.0.0.1:18080/gremlin', 'controlled-endpoint-required')
    need(not profile.get('authorizationEnv'), 'authenticated-profile-not-supported')
    need(shared['purpose'] == plan['purpose'] and 'mode' not in shared, 'shared-environment-settings')
    need(all(str(Path(shared[k]).resolve(strict=True)) == shared[k] for k in ('distribution', 'javaHome')),
         'shared-asset-paths-must-be-real')
    need(not any(root.is_relative_to(Path(shared[k])) for k in ('distribution', 'javaHome')),
         'attempt-in-target-asset-tree')
    need(runtime['driver']['image'] == shared['image'], 'driver-must-use-frozen-environment-image')
    budget, collector = plan['budgets'], runtime['collector']
    need(budget['sealAndAnalyzeMs'] >= plan['analysisPolicy']['maxMilliseconds'], 'analysis-seal-budget')
    jvm_reserve = jvm_evidence_reserve(shared, budget, plan['analysisPolicy']['maxMilliseconds'])
    need(10000 <= budget['modeProbeMs'] <= 180000, 'mode-probe-budget-outside-supported-range')
    need(collector['durationMs'] >= budget['workflowMaxMilliseconds'] + 3 * collector['intervalMs'],
         'collector-deadline-must-cover-workflow')
    need(runtime['driver']['wallSeconds'] * 1000 >= collector['durationMs'] + 30000,
         'driver-ttl-must-cover-collector-and-cleanup')
    need(shared['budget']['wallSeconds'] * 1000 >= budget['startMs'] + 2 * budget['modeProbeMs'] +
         budget['settleMs'] + collector['durationMs'] + 30000, 'target-ttl-must-cover-arm')
    need(profile['maxTraceBytes'] + collector['maxOutputBytes'] + 8 * MiB + jvm_reserve +
         4 * runtime['driver']['maxLogBytes'] < budget['maxEvidenceBytesPerArm'],
         'arm-evidence-reserve')
    root.mkdir(mode=0o700)
    copy_regular_tree(comparison, root / 'comparison', 64 * MiB, check)
    identities = {'source': inventory(source, source_selection(runtime), check),
                  'node': inventory(node, check=check),
                  'javaHome': inventory(Path(shared['javaHome']), check=check),
                  'distribution': inventory(Path(shared['distribution']), check=check),
                  'hostPython': file_entry(runtime['hostPython'], check=check),
                  'dockerExecutable': file_entry(runtime['dockerExecutable'], check=check)}
    if go:
        binary = runtime['executor']['binary']
        go_extension().binary_source_identity(identities['source'], binary)
        identities['goBinary'] = file_entry(binary['path'], check=check)
        need(identities['goBinary']['sha256'] == binary['sha256'] and
             identities['goBinary']['bytes'] <= 32 * MiB, 'Go-binary-identity-budget')
        proof_root = Path(runtime['executor']['clockProof'])
        identities['goClockProof'] = [file_entry(f, f.relative_to(proof_root).as_posix(), check)
                                      for f in regular_files(proof_root)]
        need(len(identities['goClockProof']) == 4, 'Go-clock-proof-inventory')
        copy_regular_tree(proof_root, root / 'go-clock-proof', 2 * MiB, check)
        identities['goControlFiles'] = [file_entry(Path(__file__).with_name(n), n, check)
                                        for n in go_extension().CONTROL_FILES]
        for entry in identities['goControlFiles']:
            shutil.copyfile(Path(__file__).with_name(entry['path']), root / entry['path'])
    value = {'schemaVersion': 2 if go else 1, 'kind': 'http-comparison-runtime-plan', 'planDigest': plan['planDigest'],
             'planContentDigest': verification['contentDigest'], 'settings': runtime, 'identities': identities,
             'componentSha256': file_entry(__file__)['sha256']}
    required_evidence = (profile['maxTraceBytes'] + collector['maxOutputBytes'] + 8 * MiB + jvm_reserve +
                         4 * runtime['driver']['maxLogBytes'] +
                         20 * runtime['supervisor']['maxCommandBytes'] + 2 * len(encode(value)) +
                         sum(f['bytes'] for f in read_json(root / 'comparison/target-files.json')))
    need(required_evidence <= budget['maxEvidenceBytesPerArm'], 'combined-arm-evidence-budget')
    save(root / 'runtime.json', value)
    lock = {'schemaVersion': 2 if go else 1, 'kind': 'http-comparison-runtime-lock', 'planDigest': plan['planDigest'],
            'runtime': file_entry(root / 'runtime.json', 'runtime.json')}
    save(root / 'runtime.lock.json', lock)
    shutil.copyfile(__file__, root / 'comparison-workflow.py')
    return {'attempt': str(root), 'runtimeDigest': file_entry(root / 'runtime.lock.json')['sha256'],
            'planDigest': plan['planDigest'], 'executed': False, 'performanceAccepted': False}


def load_attempt(root, verify=False, check=lambda: None):
    root = Path(root).resolve(strict=True)
    value = read_json(root / 'runtime.json', 64 * MiB)
    lock = read_json(root / 'runtime.lock.json')
    need(lock.get('kind') == 'http-comparison-runtime-lock' and
         lock.get('schemaVersion') == value.get('schemaVersion') == value.get('settings', {}).get('schemaVersion') and
         lock['runtime'] == file_entry(root / 'runtime.json', 'runtime.json') and
         lock['planDigest'] == value['planDigest'] and
         value['componentSha256'] == file_entry(root / 'comparison-workflow.py')['sha256'], 'runtime-lock-mismatch')
    validate_runtime(value['settings'])
    if verify:
        settings, ids = value['settings'], value['identities']
        verify_dependency_lock(settings['sourceRoot'])
        need(inventory(settings['sourceRoot'], source_selection(settings), check) == ids['source'] and
             inventory(settings['nodeHome'], check=check) == ids['node'] and
             file_entry(settings['hostPython'], check=check) == ids['hostPython'] and
             file_entry(settings['dockerExecutable'], check=check) == ids['dockerExecutable'],
             'runtime-identity-changed')
        if settings['schemaVersion'] == 2:
            need(file_entry(settings['executor']['binary']['path'], check=check) == ids['goBinary'],
                 'Go-binary-changed')
            need([file_entry(f, f.relative_to(root / 'go-clock-proof').as_posix(), check)
                  for f in regular_files(root / 'go-clock-proof')] == ids['goClockProof'],
                 'Go-clock-proof-changed')
            for entry in ids['goControlFiles']:
                need(file_entry(root / entry['path'], entry['path'], check) == entry, 'Go-control-file-changed')
            need(read_json(root / 'go-clock-proof/proof.json')['host']['bootId'] ==
                 Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'Go-clock-proof-host-rebooted')
        shared = read_json(root / 'comparison/inputs/settings.json')
        need(inventory(Path(shared['javaHome']), check=check) == ids['javaHome'] and
             inventory(Path(shared['distribution']), check=check) == ids['distribution'], 'target-assets-changed')
    return root, value, file_entry(root / 'runtime.lock.json')['sha256']


class Controller:
    def __init__(self, root, verify=False):
        self.started = time.monotonic()
        self.root, self.runtime, self.digest = load_attempt(root)
        self.settings = self.runtime['settings']
        self.source = Path(self.settings['sourceRoot'])
        self.node = Path(self.settings['nodeHome']) / 'bin/node'
        self.plan = read_json(self.root / 'comparison/plan.json')
        self.shared = read_json(self.root / 'comparison/inputs/settings.json')
        self.env = clean_env(self.settings['hostPython'], self.settings['nodeHome'])
        self.env['PATH'] = str(Path(self.settings['dockerExecutable']).parent) + ':' + self.env['PATH']
        self.arm_deadline = None
        self.counter = 0
        self.last_disk = 0
        self.execution = None
        self.current = None
        self.cleanup = False
        self.cleanup_deadline = None
        self.active_usage = ActiveUsage(self.root, self.settings['supervisor']['maxWorkBytes'],
                                        'work-directory-budget', self.settings['supervisor']['minFreeBytes'])
        self.seal_deadline = None
        self.preflight_failure = None
        if verify:
            try:
                load_attempt(root, True, self.guard)
            except BaseException as error:
                if not self.total_mode():
                    raise
                self.preflight_failure = error_message(error)

    def total_mode(self):
        return self.plan['budgets'].get('budgetMode') == 'total'

    def total_end(self):
        return self.started + self.plan['budgets']['maxTotalMilliseconds'] / 1000

    def active_end(self):
        return self.total_end() - (self.plan['budgets']['stopMs'] + 10000) / 1000

    def phase_end(self, maximum, final_reserve=0):
        end = time.monotonic() + maximum / 1000
        return min(end, self.total_end() - final_reserve / 1000) if self.total_mode() else end

    def running_deadline(self):
        need(not CANCELLED and not (self.root / 'stop-request.json').exists(), 'controller-cancelled')
        now = time.monotonic()
        if self.total_mode():
            need(now < self.active_end(), 'total-active-deadline')
        else:
            need(now - self.started < self.plan['budgets']['maxTotalMilliseconds'] / 1000, 'total-deadline')
        need(self.arm_deadline is None or now < self.arm_deadline, 'arm-deadline')

    def guard(self):
        if self.cleanup:
            if self.total_mode():
                end = min(self.total_end(), getattr(self, 'cleanup_deadline', None) or math.inf,
                          getattr(self, 'seal_deadline', None) or math.inf)
                need(time.monotonic() < end, 'total-cleanup-or-seal-deadline')
            return
        self.running_deadline()
        now = time.monotonic()
        if now - self.last_disk >= 1:
            self.last_disk = now
            self.active_usage.sample(self.running_deadline)

    def cmd(self, argv, timeout=None, check=True, log=None):
        seconds = (timeout or self.settings['supervisor']['commandMs']) / 1000
        if self.total_mode():
            end = min(self.total_end(), self.cleanup_deadline or math.inf,
                      getattr(self, 'seal_deadline', None) or math.inf) if self.cleanup else min(
                          self.active_end(), self.arm_deadline or math.inf)
            # TERM owner cleanup (including nested parser groups) stays within this phase and hard T.
            seconds = min(seconds, end - time.monotonic() - 3)
            need(seconds > 0, 'total-command-reserve-exhausted')
            return command(argv, seconds, self.settings['supervisor']['maxCommandBytes'], self.env,
                           self.guard, log, check, terminate_grace=3, terminate_deadline=end)
        if self.cleanup and getattr(self, 'cleanup_deadline', None) is not None:
            # Preserve one bounded stop attempt for the other owner after a first cleanup exhausts the window.
            seconds = min(seconds, max(1.0, self.cleanup_deadline - time.monotonic()))
        return command(argv, seconds,
                       self.settings['supervisor']['maxCommandBytes'], self.env, self.guard, log, check)

    def cli(self, args, timeout=None, log=None):
        return response(self.cmd([self.node, self.source / 'dist/cli.js', *args], timeout, log=log))

    def docker(self, *args, check=True, log=None, timeout=None):
        return self.cmd([self.settings['dockerExecutable'], '--host', 'unix://' + self.shared['dockerSocket'],
                         *args], timeout=timeout, check=check, log=log)

    def pause(self, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self.guard()
            time.sleep(min(0.1, max(0, end - time.monotonic())))

    def planned_arms(self):
        return [dict(arm, blockId=block['id']) for block in self.plan['blocks'] for arm in block['arms']]

    def driver_name(self, arm):
        return 'hgbench-pair-' + self.execution['executionId'][:12] + '-' + arm['runId'][-24:]

    def inspect_driver(self, arm, missing=False):
        result = self.docker('inspect', '--type', 'container', self.driver_name(arm), check=False)
        if result['exitCode']:
            need(missing and 'no such' in result['stderr'].lower(), 'driver-unavailable')
            return None
        values = json.loads(result['stdout'])
        need(len(values) == 1, 'driver-identity-ambiguous')
        info = values[0]
        labels = info.get('Config', {}).get('Labels', {})
        need(info.get('Name') == '/' + self.driver_name(arm) and
             info.get('Image') == self.settings['driver']['image'] and
             labels.get(LABEL + '.execution') == self.execution['executionId'] and
             labels.get(LABEL + '.run') == arm['runId'] and labels.get(LABEL + '.runtime') == self.digest,
             'driver-owner-identity-mismatch')
        return info

    def driver_arguments(self, arm, target, evidence):
        d = self.settings['driver']
        args = ['create', '--name', self.driver_name(arm), '--pull=never', '--restart', 'no', '--init',
                '--label', LABEL + '.execution=' + self.execution['executionId'],
                '--label', LABEL + '.run=' + arm['runId'], '--label', LABEL + '.runtime=' + self.digest,
                '--network', 'container:' + target, '--user', str(os.getuid()) + ':' + str(os.getgid()),
                '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                '--cpus', str(d['cpus']), '--memory', str(d['memoryBytes']), '--memory-swap', str(d['memoryBytes']),
                '--pids-limit', str(d['pids']), '--log-driver', 'none', '--stop-timeout', str(d['shutdownSeconds']),
                '--tmpfs', '/tmp:rw,nosuid,nodev,noexec,size=' + str(d['tmpfsBytes']),
                '--workdir', '/source', '--entrypoint', d['python']]
        mounts = [(self.source, '/source', True), (Path(self.settings['nodeHome']), '/node', True),
                  (self.root / 'comparison', '/comparison', True), (self.root, '/control', True),
                  (evidence, '/evidence', False)]
        for source, dest, readonly in mounts:
            args += ['--mount', 'type=bind,src=' + str(source) + ',dst=' + dest + (',readonly' if readonly else '')]
        if self.settings['schemaVersion'] == 2:
            binary = self.settings['executor']['binary']['path']
            args += ['--mount', 'type=bind,src=' + binary + ',dst=' + binary + ',readonly']
        return args + [d['image'], '-B', '/control/comparison-workflow.py', '_driver',
                       '--arm', arm['arm'], '--run-id', arm['runId']]

    def verify_driver(self, info, arm, target, evidence):
        d, h, c = self.settings['driver'], info['HostConfig'], info['Config']
        need(c['User'] == str(os.getuid()) + ':' + str(os.getgid()) and h['ReadonlyRootfs'] is True and
             h['NetworkMode'] == 'container:' + target and h['Memory'] == d['memoryBytes'] and
             h['MemorySwap'] == d['memoryBytes'] and h['NanoCpus'] == round(d['cpus'] * 1e9) and
             h['PidsLimit'] == d['pids'] and h['CapDrop'] == ['ALL'] and not h.get('CapAdd') and
             not h['Privileged'] and 'no-new-privileges:true' in h['SecurityOpt'] and
             h['RestartPolicy']['Name'] == 'no' and h['LogConfig']['Type'] == 'none' and
             h.get('Init') is True and not h.get('PidMode') and not h.get('Devices') and
             not h.get('PortBindings') and not h.get('PublishAllPorts'), 'driver-isolation-mismatch')
        expected = {'/source': (str(self.source), False), '/node': (self.settings['nodeHome'], False),
                    '/comparison': (str(self.root / 'comparison'), False), '/control': (str(self.root), False),
                    '/evidence': (str(evidence), True)}
        if self.settings['schemaVersion'] == 2:
            binary = self.settings['executor']['binary']['path']
            expected[binary] = (binary, False)
        need({m['Destination']: (m['Source'], m['RW']) for m in info['Mounts']} == expected,
             'driver-mount-mismatch')
        need(c['Entrypoint'] == [d['python']] and c['Cmd'] == ['-B', '/control/comparison-workflow.py',
             '_driver', '--arm', arm['arm'], '--run-id', arm['runId']] and c['WorkingDir'] == '/source',
             'driver-command-mismatch')

    def wait_ready(self, attempt, maximum):
        need(maximum > 0, 'target-start-budget-exhausted')
        end = time.monotonic() + maximum / 1000
        while True:
            self.guard()
            remaining = (end - time.monotonic()) * 1000
            need(remaining > 0, 'target-not-ready-before-deadline')
            observed = self.cli(['http', 'environment', 'status', attempt], remaining)
            need(time.monotonic() < end, 'target-not-ready-before-deadline')
            if observed.get('ready') and observed.get('state') == 'running':
                return observed
            need(not observed.get('service-result') and observed['state'] not in ('exited', 'dead') and
                 time.monotonic() < end, 'target-not-ready')
            # dispatch-recorded + no container is a valid create window; never redispatch.
            self.pause(self.settings['supervisor']['pollMs'] / 1000)

    def mode_probe(self, attempt, observed, arm, phase, logs):
        b = self.plan['budgets']['modeProbeMs']
        need(10000 <= b <= 180000, 'mode-probe-budget-outside-supported-range')
        result = self.docker('exec', '--user', str(os.getuid()) + ':' + str(os.getgid()),
                             observed['container']['id'], self.shared['python'], '-B',
                             '/control/hugegraph-mode-probe.py', '--endpoint', observed['ready']['endpoint'],
                             '--jcmd', observed['ready']['jcmdExecutable'], '--java-pid',
                             str(observed['ready']['java']['pid']), '--expected-mode', arm['expectedMode'],
                             '--total-timeout-seconds', str(b / 1000), '--out', '/work/mode-' + phase,
                             timeout=b + 2000, log=logs / ('probe-' + phase + '.json'))
        need(result['exitCode'] == 0 and read_json(attempt / 'work' / ('mode-' + phase) / 'result.json')
             .get('status') == 'passed', 'mode-unverified')

    def find_host_java(self, init_pid, ready):
        namespace = os.readlink('/proc/' + str(init_pid) + '/ns/pid')
        matches = []
        for path in Path('/proc').iterdir():
            if not path.name.isdigit():
                continue
            try:
                if os.readlink(path / 'ns/pid') != namespace:
                    continue
                status = proc_read(path / 'status')
                match = re.search(r'^NSpid:\s+(.+)$', status, re.M)
                if not match or int(match.group(1).split()[-1]) != ready['java']['pid']:
                    continue
                ticks = proc_read(path / 'stat').rsplit(')', 1)[1].split()[19]
                if str(ready['java']['startTimeTicks']) == ticks:
                    matches.append(int(path.name))
            except (OSError, ValueError):
                continue
        need(len(matches) == 1, 'host-java-pid-unverified')
        return matches[0]

    def task_terminal(self, task_root, request_id, deadline, log):
        end = time.monotonic() + deadline / 1000
        if getattr(self, 'cleanup_deadline', None) is not None:
            end = min(end, self.cleanup_deadline)
        while True:
            need(time.monotonic() < end, 'task-terminal-deadline')
            row = self.cli(['--root', task_root, 'task', 'status', '--request-id', request_id])
            if row['terminal']:
                save(log, row)
                return row
            need(time.monotonic() < end, 'task-terminal-deadline')
            self.pause(self.settings['supervisor']['pollMs'] / 1000)

    def stop_target(self, work, logs):
        attempt = work / 'environment'
        if (attempt / 'plan.json').exists():
            self.cli(['http', 'environment', 'stop', attempt], self.plan['budgets']['stopMs'],
                     logs / 'target-stop.json')
            if (work / 'target-task-dispatch.json').exists():
                result = self.task_terminal(work / 'target-tasks', 'target-' + work.name,
                                            self.plan['budgets']['stopMs'], logs / 'target-task-final.json')
                need(result['state'] == 'completed' and not result['interrupted'], 'target-task-failed')
            observed = self.cli(['http', 'environment', 'status', attempt])
            info = observed.get('container')
            if info:
                need(not info['state']['Running'], 'target-still-running')
                self.docker('rm', info['id'])

    def stop_driver(self, arm, logs):
        info = self.inspect_driver(arm, missing=True)
        if info is None:
            return
        if info['State']['Running']:
            self.docker('stop', '--time', str(self.settings['driver']['shutdownSeconds']), info['Id'])
        final = self.inspect_driver(arm)
        save(logs / 'driver-final.json', final)
        need(not final['State']['Running'], 'driver-still-running')
        self.docker('rm', final['Id'])

    def archive_environment(self, attempt, destination, cap, check=lambda: None):
        destination.mkdir()
        if not (attempt / 'plan.json').exists():
            return
        plan = read_json(attempt / 'plan.json')
        names = ['plan.json', 'plan.lock.json', 'container-before-start.json', 'container-final.json',
                 'supervisor-result.json', 'dispatch.json', 'stop-request.json', 'formal-controls.json',
                 'work/ready.json', 'work/service-result.json', 'work/java-process.json']
        names += ['work/' + phase + '-capture.json' for phase in ('java-version', 'init', 'server')]
        if plan['settings'].get('jvmObservation'):
            names += ['work/server.log', 'work/jvm-launch.json', 'work/jvm-runtime.json', 'work/jvm-capture.json']
        names += [f['path'] for f in plan['controlFiles']]
        total = 0
        for name in names:
            check()
            path = attempt / name
            if path.exists():
                raw = read_bytes(path, cap)
                total += len(raw)
                need(total <= cap, 'environment-archive-budget')
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open('xb') as stream:
                    stream.write(raw)
        for phase in ('pre', 'post'):
            path = attempt / 'work' / ('mode-' + phase)
            if path.exists():
                (destination / 'work').mkdir(exist_ok=True)
                copy_regular_tree(path, destination / 'work' / path.name, cap - total, check)
                total += sum(p.stat().st_size for p in regular_files(destination / 'work' / path.name))

    def parse_jvm(self, sealed, logs, seal_deadline, remaining, check):
        spec = self.shared.get('jvmObservation')
        if spec is None:
            return
        environment = sealed / 'environment'
        work = environment / 'work'
        # Missing inputs belong to a failed arm; preserve the available environment as-is.
        inputs = ['server.log', 'jvm-launch.json', 'jvm-runtime.json', 'jvm-capture.json', 'server-capture.json']
        need(all((work / name).is_file() for name in inputs), 'jvm-observation-input-missing')
        need(remaining() > spec['parser']['maxOutputBytes'], 'jvm-observation-output-reserve')
        timeout = min(spec['parser']['timeoutMs'] / 1000, seal_deadline - time.monotonic())
        need(timeout > 0, 'jvm-parser-seal-budget')
        result = command([self.settings['hostPython'], '-B', environment / 'jvm-observation.py', '--json',
                          'parse', '--plan', environment / 'jvm-observation', '--raw', work / 'server.log',
                          '--launch', work / 'jvm-launch.json', '--capture', work / 'jvm-capture.json',
                          '--out', sealed / 'jvm'], timeout, self.settings['supervisor']['maxCommandBytes'],
                         self.env, check, logs / 'jvm-parse-command.json', False)
        need(result['exitCode'] in (0, 2), 'jvm-parser-failed-see-command')
        reply = json.loads(result['stdout'])
        need(reply.get('data', {}).get('status') in ('completed', 'partial'), 'jvm-parser-receipt')
        remaining()

    def arm(self, arm):
        self.guard()
        started = ns()
        self.arm_deadline = time.monotonic() + arm['maxMilliseconds'] / 1000
        work = self.root / 'work' / arm['runId']
        work.mkdir()
        sealed = self.root / 'execution/arms' / arm['runId']
        sealed.mkdir()
        logs = sealed / 'controller'
        logs.mkdir()
        for name in ('runtime.json', 'runtime.lock.json'):
            shutil.copyfile(self.root / name, logs / name)
        evidence = work / 'driver'
        evidence.mkdir()
        self.current = {'arm': arm, 'work': str(work)}
        save(self.root / 'current.json', self.current, replace=True)
        settings = dict(self.shared, mode=arm['expectedMode'])
        save(work / 'settings.json', settings)
        collector = None
        go_observer = None
        failure = None
        workflow = None
        try:
            planned = self.cli(['http', 'environment', 'plan', work / 'settings.json', '--out', work / 'environment'],
                               self.plan['budgets']['prepareMs'], logs / 'target-plan.json')
            actual = read_json(work / 'environment/plan.json')
            need(actual['settings'] == settings, 'environment-settings-drift')
            need(actual['sources']['javaHome']['files'] == self.runtime['identities']['javaHome'] and
                 actual['sources']['distribution']['files'] == self.runtime['identities']['distribution'],
                 'environment-assets-drift')
            build = read_json(self.root / 'comparison/inputs/build.json')
            build_files = lambda files: sorted((f['path'], f['bytes'], f['sha256']) for f in files
                                               if f.get('type', 'file') == 'file')
            need(build_files(actual['sources']['distribution']['files']) == build_files(build['files']),
                 'target-distribution-differs')
            start_deadline = time.monotonic() + self.plan['budgets']['startMs'] / 1000
            save(work / 'target-task-dispatch.json', {'runId': arm['runId'], 'atMonotonicNs': ns()})
            remaining = (start_deadline - time.monotonic()) * 1000
            need(remaining > 0, 'target-start-budget-exhausted')
            self.cli(['--root', work / 'target-tasks', 'task', 'run', '--plan', planned['taskPlan'], '--detach',
                      '--request-id', 'target-' + arm['runId']], remaining, logs / 'target-task-run.json')
            observed = self.wait_ready(work / 'environment', (start_deadline - time.monotonic()) * 1000)
            save(logs / 'target-ready-status.json', observed)
            self.mode_probe(work / 'environment', observed, arm, 'pre', logs)
            self.pause(self.plan['budgets']['settleMs'] / 1000)
            save(work / 'driver-dispatch.json', {'name': self.driver_name(arm), 'runId': arm['runId'],
                                                'atMonotonicNs': ns(), 'runtimeDigest': self.digest})
            args = self.driver_arguments(arm, observed['container']['id'], evidence)
            self.docker(*args, log=logs / 'driver-create.json')
            info = self.inspect_driver(arm)
            self.verify_driver(info, arm, observed['container']['id'], evidence)
            save(logs / 'driver-before-start.json', info)
            self.docker('start', info['Id'])
            ready_deadline = time.monotonic() + min(60, self.settings['driver']['wallSeconds'])
            while not (evidence / 'driver-ready.json').exists():
                need(self.inspect_driver(arm)['State']['Running'] and time.monotonic() < ready_deadline,
                     'driver-not-ready')
                self.pause(0.1)
            need(time.monotonic() < ready_deadline, 'driver-ready-arrived-after-deadline')
            info = self.inspect_driver(arm)
            save(logs / 'driver-started.json', info)
            target = json.loads(self.docker('inspect', observed['container']['id'])['stdout'])[0]
            target_pid = self.find_host_java(target['State']['Pid'], observed['ready'])
            driver_ready = read_json(evidence / 'driver-ready.json')
            need(driver_ready['runId'] == arm['runId'] and
                 driver_ready['bootId'] == self.execution['hostBootId'], 'driver-ready-identity')
            supervisor_pid = self.find_host_java(info['State']['Pid'], {'java': {
                'pid': driver_ready['pid'], 'startTimeTicks': driver_ready['startTimeTicks']}})
            save(logs / 'process-identities.json', {
                'schemaVersion': 1, 'kind': 'http-comparison-process-identities',
                'hostBootId': self.execution['hostBootId'], 'recordedMonotonicNs': ns(),
                'roles': [process_identity('target', target_pid), process_identity('driver', info['State']['Pid']),
                          process_identity('driver-supervisor', supervisor_pid)]})
            collector = self.start_collector(arm, work, sealed, target_pid, info['State']['Pid'])
            if self.settings['schemaVersion'] == 2:
                profile = read_json(self.root / 'comparison/profiles' / (arm['arm'] + '.json'))
                go_observer = go_extension().ProcessObserver(profile, info['Id'], evidence / 'attempt/output',
                    logs / 'go-process-observations', lambda args: self.docker(*args)['stdout'], self.guard)
            save(evidence / 'go.json', {'runId': arm['runId'], 'runtimeDigest': self.digest, 'atMonotonicNs': ns()})
            while not (evidence / 'workflow-result.json').exists():
                self.guard()
                if go_observer is not None:
                    go_observer.poll()
                need(collector.poll() is None, 'collector-ended-before-workflow')
                need(self.inspect_driver(arm)['State']['Running'], 'driver-ended-before-workflow')
                self.pause(self.settings['supervisor']['pollMs'] / 1000)
            workflow = read_json(evidence / 'workflow-result.json')
            need(workflow['ok'] is True and workflow['runId'] == arm['runId'], 'driver-workflow-failed')
            if go_observer is not None:
                save(logs / 'go-process-summary.json', go_observer.finish())
            self.finish_collector(collector, arm, work, sealed, workflow)
            collector = None
            self.mode_probe(work / 'environment', observed, arm, 'post', logs)
            save(evidence / 'release.json', {'runId': arm['runId'], 'atMonotonicNs': ns()})
            end = time.monotonic() + self.settings['driver']['shutdownSeconds']
            while self.inspect_driver(arm)['State']['Running']:
                need(time.monotonic() < end, 'driver-release-deadline')
                self.pause(0.1)
            final = self.inspect_driver(arm)
            need(final['State']['ExitCode'] == 0 and not final['State']['OOMKilled'], 'driver-container-failed')
        except BaseException as error:
            failure = error_message(error)
        finally:
            self.cleanup = True
            cleanup_started = time.monotonic()
            self.cleanup_deadline = self.phase_end(self.plan['budgets']['stopMs'], 10000)
            errors = []
            if collector is not None:
                try:
                    collector.terminate()
                    wait = 3 if not self.total_mode() else max(0, min(3, self.cleanup_deadline - time.monotonic()))
                    collector.wait(timeout=wait)
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    kill_group(collector)
            # Independent cleanup: failure of one never skips the other.
            for cleanup in (lambda: self.stop_driver(arm, logs), lambda: self.stop_target(work, logs)):
                try:
                    cleanup()
                except BaseException as error:
                    errors.append(str(error))
            if time.monotonic() > self.cleanup_deadline:
                errors.append('stop-phase-budget-exceeded')
            self.cleanup_deadline = None
            seal_deadline = self.phase_end(self.plan['budgets'].get('sealAndAnalyzeMs', 30000), 1000)
            if self.total_mode():
                self.seal_deadline = seal_deadline
            def seal_check():
                need(time.monotonic() < seal_deadline, 'seal-phase-budget-exceeded')
            def remaining():
                seal_check()
                used = sum(p.stat().st_size for p in regular_files(sealed))
                room = arm['maxEvidenceBytes'] - used - 65536
                need(room > 0 and shutil.disk_usage(self.root).free >= self.settings['supervisor']['minFreeBytes'],
                     'archive-storage-budget')
                return room
            try:
                save(logs / 'host-active-usage.json', self.active_usage.audit)
                if self.settings['schemaVersion'] == 2:
                    copy_regular_tree(self.root / 'go-clock-proof', logs / 'go-clock-proof', remaining(), seal_check)
                    for name in go_extension().CONTROL_FILES:
                        raw = read_bytes(self.root / name, remaining())
                        with (logs / name).open('xb') as stream:
                            stream.write(raw)
                if (evidence / 'attempt/output').is_dir():
                    copy_regular_tree(evidence / 'attempt/output', sealed / 'raw', remaining(), seal_check)
            except BaseException as error:
                errors.append(str(error))
            try:
                self.archive_environment(work / 'environment', sealed / 'environment', remaining(), seal_check)
            except BaseException as error:
                errors.append(str(error))
            try:
                self.parse_jvm(sealed, logs, seal_deadline, remaining, seal_check)
            except BaseException as error:
                errors.append(str(error))
            try:
                for name in ('workflow-result.json', 'driver-ready.json', 'go.json', 'release.json',
                             'driver-active-usage.json',
                             'http-plan-command.json', 'http-task-command.json', 'http-artifacts-command.json',
                             'http-cancel-command.json'):
                    if (evidence / name).exists():
                        raw = read_bytes(evidence / name, remaining())
                        with (logs / name).open('xb') as stream:
                            stream.write(raw)
            except BaseException as error:
                errors.append(str(error))
            if time.monotonic() > seal_deadline:
                errors.append('seal-phase-budget-exceeded')
            if self.arm_deadline is not None and time.monotonic() > self.arm_deadline:
                errors.append('arm-budget-exceeded-during-cleanup')
            raw = sealed / 'raw/raw.lock.json'
            env_plan = sealed / 'environment/plan.json'
            receipt = {'schemaVersion': 1, 'kind': 'http-comparison-arm-receipt',
                       'executionId': self.execution['executionId'], 'planDigest': self.plan['planDigest'],
                       **{k: arm[k] for k in ('runId', 'blockId', 'arm', 'position')},
                       'armStartedMonotonicNs': started, 'armFinishedMonotonicNs': ns(),
                       'workflowStartedMonotonicNs': workflow.get('startedMonotonicNs') if workflow else None,
                       'workflowFinishedMonotonicNs': workflow.get('finishedMonotonicNs') if workflow else None,
                       'rawDirectory': 'arms/' + arm['runId'] + '/raw',
                       'rawLockSha256': file_entry(raw)['sha256'] if raw.exists() else None,
                       'environmentDirectory': 'arms/' + arm['runId'] + '/environment',
                       'environmentPlanSha256': file_entry(env_plan)['sha256'] if env_plan.exists() else None,
                       'failure': failure, 'cleanupErrors': errors, 'performanceAccepted': False}
            if self.total_mode():
                receipt.update(budgetMode='total', partial=failure is not None or bool(errors),
                               totalDeadlineMonotonicNs=str(int(self.total_end() * 1e9)))
            save(sealed / 'receipt.json', receipt)
            self.cleanup = False
            self.arm_deadline = None
            self.seal_deadline = None
        need(failure is None and not errors, 'arm-failed-see-preserved-receipt')

    def run(self):
        if self.total_mode():
            return self.run_total()
        need(platform.system() == 'Linux' and os.getuid() > 0 and os.getgid() > 0, 'nonroot-linux-required')
        need(not (self.root / 'stop-request.json').exists(), 'attempt-already-stopped')
        need(file_entry(__file__)['sha256'] == self.runtime['componentSha256'], 'running-controller-not-frozen')
        # Exclusive claim remains after any failure; never redispatch an interrupted experiment.
        save(self.root / 'dispatch.json', {'atMonotonicNs': ns(), 'pid': os.getpid(), 'runtimeDigest': self.digest})
        boot = proc_read('/proc/sys/kernel/random/boot_id', 128).strip()
        expected = read_json(self.root / 'comparison/inputs/environment.json')
        need(boot == expected['hostBootId'] and platform.release() == expected['kernel']['release'] and
             platform.machine() == expected['kernel']['architecture'], 'host-environment-changed')
        actual_node = self.cmd([self.node, '--version'])['stdout'].strip()
        actual_docker = json.loads(self.docker('version', '--format', '{{json .Server}}')['stdout'])
        need(actual_node == expected['node']['version'] and actual_docker['Version'] ==
             expected['docker']['serverVersion'], 'runtime-version-changed')
        verified = self.cli(['http', 'comparison', 'verify', self.root / 'comparison'], 300000)
        need(verified['planDigest'] == self.runtime['planDigest'] and
             verified['contentDigest'] == self.runtime['planContentDigest'], 'comparison-lock-changed')
        (self.root / 'work').mkdir()
        (self.root / 'execution').mkdir()
        (self.root / 'execution/arms').mkdir()
        self.execution = {'schemaVersion': 1, 'kind': 'http-comparison-execution',
                          'executionId': uuid.uuid4().hex, 'planDigest': self.plan['planDigest'],
                          'planContentDigest': verified['contentDigest'], 'hostBootId': boot,
                          'registeredMonotonicNs': ns(), 'runtimeDigest': self.digest,
                          'receipts': [{'runId': a['runId'], 'path': 'arms/' + a['runId'] + '/receipt.json'}
                                       for a in self.planned_arms()]}
        save(self.root / 'execution/execution.json', self.execution)
        failure = None
        reverified = False
        try:
            for arm in self.planned_arms():
                self.arm(arm)
            load_attempt(self.root, verify=True, check=self.guard)
            reverified = True
        except BaseException as error:
            failure = error_message(error)
        finally:
            self.cleanup = True
            save(self.root / 'execution/terminal.json', {
                'schemaVersion': 1, 'kind': 'http-comparison-execution-terminal',
                'executionId': self.execution['executionId'], 'planDigest': self.plan['planDigest'],
                'runtimeDigest': self.digest, 'status': 'completed' if failure is None else 'failed',
                'failure': failure, 'runtimeReverified': reverified, 'finishedMonotonicNs': ns(),
                'performanceAccepted': False})
            seal_end = min(self.started + self.plan['budgets']['maxTotalMilliseconds'] / 1000,
                           time.monotonic() + self.plan['budgets']['sealAndAnalyzeMs'] / 1000)
            def seal_check():
                need(time.monotonic() < seal_end, 'execution-seal-deadline')
            files = [file_entry(p, p.relative_to(self.root / 'execution').as_posix(), seal_check)
                     for p in regular_files(self.root / 'execution')]
            need(sum(f['bytes'] for f in files) <= self.plan['budgets']['maxTotalEvidenceBytes'],
                 'execution-evidence-budget')
            save(self.root / 'execution/execution.lock.json',
                 {'schemaVersion': 1, 'kind': 'http-comparison-execution-lock', 'planDigest': self.plan['planDigest'],
                  'files': files, 'contentDigest': value_hash(files)})
            bridge = None
            evidence_valid = False
            bridge_error = None
            try:
                remaining_ms = self.plan['budgets']['maxTotalMilliseconds'] - (time.monotonic() - self.started) * 1000
                seal_ms = (seal_end - time.monotonic()) * 1000
                need(min(remaining_ms, seal_ms) > 0, 'total-or-seal-budget-exhausted-before-analysis')
                bridge = self.cmd([self.node, self.source / 'components/http/paired-evidence.mjs', '--plan',
                                   self.root / 'comparison', '--execution', self.root / 'execution', '--out',
                                   self.root / 'analysis'],
                                  min(remaining_ms, seal_ms),
                                  check=False, log=self.root / 'analysis-command.json')
                if bridge['exitCode'] == 0:
                    evidence_valid = response(bridge).get('valid') is True
            except BaseException as error:
                bridge_error = error_message(error)
            value = {'kind': 'http-comparison-workflow-result',
                     'ok': failure is None and evidence_valid,
                     'executionCompleted': failure is None, 'evidenceValid': evidence_valid,
                     'statisticsEligible': (response(bridge).get('statisticsEligible') is True
                                            if bridge and bridge['exitCode'] == 0 and evidence_valid else False),
                     'failure': failure, 'bridgeError': bridge_error,
                     'bridgeExitCode': bridge['exitCode'] if bridge else None, 'finishedMonotonicNs': ns(),
                     'performanceAccepted': False, 'inferenceAccepted': False}
            save(self.root / 'result.json', value)
        return value


    def run_total(self):
        """Versioned total budget: work, all cleanup and retained partial evidence share T."""
        need(platform.system() == 'Linux' and os.getuid() > 0 and os.getgid() > 0, 'nonroot-linux-required')
        need(not (self.root / 'stop-request.json').exists(), 'attempt-already-stopped')
        need(file_entry(__file__)['sha256'] == self.runtime['componentSha256'], 'running-controller-not-frozen')
        save(self.root / 'dispatch.json', {'atMonotonicNs': ns(), 'pid': os.getpid(), 'runtimeDigest': self.digest})
        (self.root / 'work').mkdir()
        (self.root / 'execution').mkdir()
        (self.root / 'execution/arms').mkdir()
        boot = proc_read('/proc/sys/kernel/random/boot_id', 128).strip()
        self.execution = {'schemaVersion': 1, 'kind': 'http-comparison-execution',
                          'executionId': uuid.uuid4().hex, 'planDigest': self.plan['planDigest'],
                          'planContentDigest': self.runtime['planContentDigest'], 'hostBootId': boot,
                          'registeredMonotonicNs': ns(), 'runtimeDigest': self.digest,
                          'receipts': [{'runId': a['runId'], 'path': 'arms/' + a['runId'] + '/receipt.json'}
                                       for a in self.planned_arms()]}
        # Preflight failures also retain the whole predeclared denominator.
        save(self.root / 'execution/execution.json', self.execution)
        failure, reverified, bridge, bridge_error = self.preflight_failure, False, None, None
        evidence_valid, sealed = False, False
        try:
            need(failure is None, failure or 'preflight-failed')
            self.guard()
            expected = read_json(self.root / 'comparison/inputs/environment.json')
            need(boot == expected['hostBootId'] and platform.release() == expected['kernel']['release'] and
                 platform.machine() == expected['kernel']['architecture'], 'host-environment-changed')
            actual_node = self.cmd([self.node, '--version'])['stdout'].strip()
            actual_docker = json.loads(self.docker('version', '--format', '{{json .Server}}')['stdout'])
            need(actual_node == expected['node']['version'] and actual_docker['Version'] ==
                 expected['docker']['serverVersion'], 'runtime-version-changed')
            verified = self.cli(['http', 'comparison', 'verify', self.root / 'comparison'], 300000)
            need(verified['planDigest'] == self.runtime['planDigest'] and
                 verified['contentDigest'] == self.runtime['planContentDigest'], 'comparison-lock-changed')
            for arm in self.planned_arms():
                self.arm(arm)
            # All owned live work is gone; final verification can use remaining T,
            # without reserving another stop allowance or resetting the seal phase.
            self.cleanup = True
            self.seal_deadline = self.phase_end(self.plan['budgets']['sealAndAnalyzeMs'], 1000)
            load_attempt(self.root, verify=True, check=self.guard)
            reverified = True
            need(not CANCELLED and not (self.root / 'stop-request.json').exists(), 'controller-cancelled')
        except BaseException as error:
            failure = error_message(error)
        finally:
            self.cleanup = True
            self.cleanup_deadline = None  # Every dispatched arm already attempted its owned cleanup.
            self.seal_deadline = min(self.seal_deadline or math.inf,
                                     self.phase_end(self.plan['budgets']['sealAndAnalyzeMs'], 1000))
            save(self.root / 'execution/terminal.json', {
                'schemaVersion': 1, 'kind': 'http-comparison-execution-terminal',
                'executionId': self.execution['executionId'], 'planDigest': self.plan['planDigest'],
                'runtimeDigest': self.digest, 'status': 'completed' if failure is None else 'failed',
                'failure': failure, 'runtimeReverified': reverified, 'finishedMonotonicNs': ns(),
                'performanceAccepted': False})
            try:
                files = [file_entry(p, p.relative_to(self.root / 'execution').as_posix(), self.guard)
                         for p in regular_files(self.root / 'execution')]
                need(sum(f['bytes'] for f in files) <= self.plan['budgets']['maxTotalEvidenceBytes'],
                     'execution-evidence-budget')
                self.guard()
                save(self.root / 'execution/execution.lock.json',
                     {'schemaVersion': 1, 'kind': 'http-comparison-execution-lock', 'planDigest': self.plan['planDigest'],
                      'files': files, 'contentDigest': value_hash(files)})
                sealed = True
                remaining_ms = (self.seal_deadline - time.monotonic()) * 1000
                need(remaining_ms > 3000, 'total-seal-budget-exhausted-before-analysis')
                bridge = self.cmd([self.node, self.source / 'components/http/paired-evidence.mjs', '--plan',
                                   self.root / 'comparison', '--execution', self.root / 'execution', '--out',
                                   self.root / 'analysis'], remaining_ms, check=False,
                                  log=self.root / 'analysis-command.json')
                if bridge['exitCode'] == 0:
                    evidence_valid = response(bridge).get('valid') is True
            except BaseException as error:
                bridge_error = error_message(error)
            if CANCELLED or (self.root / 'stop-request.json').exists():
                failure = failure or 'controller-cancelled'
            complete = failure is None
            value = {'kind': 'http-comparison-workflow-result', 'ok': complete and evidence_valid,
                     'executionCompleted': complete, 'evidenceValid': evidence_valid,
                     'statisticsEligible': (response(bridge).get('statisticsEligible') is True
                                            if bridge and bridge['exitCode'] == 0 and evidence_valid else False),
                     'failure': failure, 'bridgeError': bridge_error,
                     'bridgeExitCode': bridge['exitCode'] if bridge else None, 'finishedMonotonicNs': ns(),
                     'budgetMode': 'total', 'partial': not (complete and evidence_valid),
                     'executionSealed': sealed, 'plannedArms': len(self.planned_arms()),
                     'totalDeadlineMonotonicNs': str(int(self.total_end() * 1e9)),
                     'performanceAccepted': False, 'inferenceAccepted': False}
            save(self.root / 'result.json', value)
        return value


def proc_read(path, cap=65536):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(cap + 1)
    need(len(raw) <= cap, 'proc-read-budget')
    return raw.decode()


def process_identity(role, pid):
    root = Path('/proc') / str(pid)
    stat_raw = proc_read(root / 'stat')
    value = {'role': role, 'hostPid': pid, 'statRaw': stat_raw,
             'statusRaw': proc_read(root / 'status'), 'cgroupRaw': proc_read(root / 'cgroup'),
             'namespaces': {name: os.readlink(root / 'ns' / name) for name in ('pid', 'net', 'mnt', 'user')}}
    need(stat_raw.rsplit(')', 1)[1].split()[19] == proc_read(root / 'stat').rsplit(')', 1)[1].split()[19],
         'process-identity-changed-during-capture')
    return value


def publish_marker(path, value):
    """Atomic visibility and no replacement, including races with another stop caller."""
    path = Path(path)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    save(temporary, value)
    try:
        os.link(temporary, path)
    finally:
        temporary.unlink()


def verify_collector(directory, workflow=None):
    directory = Path(directory)
    manifest = read_json(directory / 'manifest.json')
    need(manifest.get('kind') == 'http-resource-evidence' and manifest.get('sealed') is True and
         manifest.get('collectionStatus') == 'completed', 'collector-unsealed-or-partial')
    need({f['path'] for f in manifest['files']} == {'spec.json', 'samples.ndjson', 'summary.json'} and
         len(manifest['files']) == 3, 'collector-manifest-inventory')
    for entry in manifest['files']:
        need(file_entry(directory / entry['path'], entry['path']) == entry, 'collector-manifest-digest')
    summary = read_json(directory / 'summary.json')
    need(summary['status'] == 'completed' and summary['reason'] == 'controlled-stop', 'collector-stop-not-complete')
    if workflow:
        sample = first_sample(directory / 'samples.ndjson')
        need(sample is not None and int(sample['finishedMonotonicNs']) <= int(workflow['startedMonotonicNs']) and
             int(summary['lastSampleFinishedMonotonicNs']) > int(workflow['finishedMonotonicNs']),
             'collector-workflow-window-uncovered')
    return summary


def first_sample(path):
    if not path.exists():
        return None
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.readline(2 * MiB)
    if not raw.endswith(b'\n'):
        return None
    sample = json.loads(raw)
    need(sample.get('kind') == 'http-resource-sample', 'collector-first-sample-invalid')
    return sample


def start_collector(self, arm, work, sealed, target_pid, driver_pid):
    spec = {'schemaVersion': 1, 'kind': 'http-resource-collection', **self.settings['collector'],
            'filesystemPath': str(self.root), 'roles': [{'role': 'target', 'hostPid': target_pid},
                                                       {'role': 'driver', 'hostPid': driver_pid}],
            'controlledStop': {'path': str(work / 'collector-stop.json'), 'token': uuid.uuid4().hex}}
    save(work / 'collector-spec.json', spec)
    logs = sealed / 'controller'
    save(logs / 'collector-roles.json', {'runId': arm['runId'], 'targetJavaHostPid': target_pid,
                                       'driverInitHostPid': driver_pid, 'spec': spec})
    stream = (logs / 'collector-command.log').open('xb')
    argv = [self.settings['hostPython'], '-B', self.source / 'components/http/resource-collector.py',
            'collect', '--spec', work / 'collector-spec.json', '--out', sealed / 'collector', '--json']
    process = subprocess.Popen(argv,
                               stdout=stream, stderr=subprocess.STDOUT, env=self.env, start_new_session=True)
    stream.close()
    try:
        end = time.monotonic() + min(30, self.settings['collector']['durationMs'] / 1000)
        while first_sample(sealed / 'collector/samples.ndjson') is None:
            self.guard()
            need(process.poll() is None and time.monotonic() < end, 'collector-start-failed')
            self.pause(0.05)
        return process
    except BaseException:
        try:
            process.terminate()
            process.wait(timeout=3)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            kill_group(process)
        raise


def finish_collector(self, process, arm, work, sealed, workflow):
    spec = read_json(sealed / 'collector/spec.json')
    marker = {'schemaVersion': 1, 'kind': 'http-resource-stop', 'token': spec['controlledStop']['token'],
              'specSha256': file_entry(sealed / 'collector/spec.json')['sha256'],
              'hostBootId': self.execution['hostBootId'],
              'workflowFinishedMonotonicNs': workflow['finishedMonotonicNs'], 'stopRequestedMonotonicNs': ns()}
    publish_marker(work / 'collector-stop.json', marker)
    end = time.monotonic() + 2 * spec['intervalMs'] / 1000 + 10
    while process.poll() is None:
        need(time.monotonic() < end, 'collector-stop-deadline')
        self.pause(0.05)
    need(time.monotonic() < end, 'collector-stop-arrived-after-deadline')
    need(process.returncode == 0, 'collector-failed-see-summary')
    summary = verify_collector(sealed / 'collector', workflow)
    stop_raw = read_bytes(work / 'collector-stop.json', 4096)
    need(summary['controlledStop']['request'] == marker and
         summary['controlledStop']['sha256'] == sha(stop_raw) and
         type(summary['controlledStop'].get('coveringSampleIndex')) is int and
         summary['componentSha256'] == file_entry(self.source / 'components/http/resource-collector.py')['sha256'],
         'collector-terminal-identity')
    with (sealed / 'controller/collector-stop.json').open('xb') as stream:
        stream.write(stop_raw)
    save(sealed / 'controller/collector-window.json',
         {'runId': arm['runId'], 'workflowStartedMonotonicNs': workflow['startedMonotonicNs'],
          'workflowFinishedMonotonicNs': workflow['finishedMonotonicNs'], 'stopMarker': marker,
          'summarySha256': file_entry(sealed / 'collector/summary.json')['sha256'],
          'firstSampleStartedMonotonicNs': summary['firstSampleStartedMonotonicNs'],
          'lastSampleFinishedMonotonicNs': summary['lastSampleFinishedMonotonicNs'],
          'clock': 'same Linux boot CLOCK_MONOTONIC via Python time.monotonic_ns',
          'measurementPhaseAbsoluteAnchors': False, 'performanceAccepted': False})


Controller.start_collector = start_collector
Controller.finish_collector = finish_collector


def driver(arm, run_id):
    control, evidence = Path('/control'), Path('/evidence')
    runtime = read_json(control / 'runtime.json', 64 * MiB)
    lock = read_json(control / 'runtime.lock.json')
    need(lock['runtime'] == file_entry(control / 'runtime.json', 'runtime.json'), 'driver-runtime-lock')
    settings = runtime['settings']
    plan = read_json('/comparison/plan.json')
    scheduled = [a for b in plan['blocks'] for a in b['arms'] if a['runId'] == run_id and a['arm'] == arm]
    need(len(scheduled) == 1 and plan['planDigest'] == runtime['planDigest'], 'driver-arm-identity')
    start = time.monotonic()
    budget = settings['driver']
    env = clean_env(budget['python'], '/node')
    started = None
    task_dispatched = False
    success = False
    last_scan = [0.0]
    active_usage = ActiveUsage(evidence, scheduled[0]['maxEvidenceBytes'], 'driver-evidence-budget',
                               settings['supervisor']['minFreeBytes'])

    def deadline():
        need(not CANCELLED and not (control / 'stop-request.json').exists(), 'driver-cancelled')
        need(time.monotonic() - start < budget['wallSeconds'], 'driver-ttl-exceeded')

    def guard():
        deadline()
        if time.monotonic() - last_scan[0] < 1:
            return
        last_scan[0] = time.monotonic()
        active_usage.sample(deadline)

    def cli(name, args, timeout):
        return response(command(['/node/bin/node', '/source/dist/cli.js', *args], timeout,
                                budget['maxLogBytes'], env, guard, evidence / name))

    try:
        need(file_entry('/control/comparison-workflow.py')['sha256'] == runtime['componentSha256'],
             'driver-component-changed')
        verify_dependency_lock('/source')
        need(inventory('/source', source_selection(settings), guard) == runtime['identities']['source'] and
             inventory('/node', check=guard) == runtime['identities']['node'], 'driver-runtime-content-changed')
        if settings['schemaVersion'] == 2:
            need(file_entry(settings['executor']['binary']['path'], check=guard) == runtime['identities']['goBinary'],
                 'driver-Go-binary-changed')
        start_ticks = proc_read('/proc/self/stat').rsplit(')', 1)[1].split()[19]
        save(evidence / 'driver-ready.json', {'runId': run_id, 'atMonotonicNs': ns(), 'pid': os.getpid(),
                                             'bootId': proc_read('/proc/sys/kernel/random/boot_id').strip(),
                                             'startTimeTicks': start_ticks})
        while not (evidence / 'go.json').exists():
            guard()
            time.sleep(0.05)
        go = read_json(evidence / 'go.json')
        need(go['runId'] == run_id and go['runtimeDigest'] == file_entry(control / 'runtime.lock.json')['sha256'],
             'driver-go-identity')
        started = ns()
        planned = cli('http-plan-command.json', ['http', 'plan', '/comparison/profiles/' + arm + '.json',
                                                '--out', '/evidence/attempt'], 60)
        task_dispatched = True
        task = cli('http-task-command.json', ['--root', '/evidence/tasks', 'task', 'run', '--plan',
                   planned['taskPlan'], '--request-id', run_id], plan['budgets']['workflowMaxMilliseconds'] / 1000)
        need(task['state'] == 'completed' and not task['interrupted'], 'http-workflow-task-failed')
        cli('http-artifacts-command.json',
            ['--root', '/evidence/tasks', 'task', 'artifacts', '--id', task['taskId']], 30)
        need((evidence / 'attempt/output/completed.json').is_file(), 'http-workflow-incomplete')
        need(time.monotonic_ns() - int(started) <= plan['budgets']['workflowMaxMilliseconds'] * 1000000,
             'whole-workflow-deadline')
        save(evidence / 'workflow-result.json', {'runId': run_id, 'ok': True, 'taskId': task['taskId'],
                                                'startedMonotonicNs': started, 'finishedMonotonicNs': ns(),
                                                'performanceAccepted': False})
        success = True
        while not (evidence / 'release.json').exists():
            guard()
            time.sleep(0.05)
        need(read_json(evidence / 'release.json')['runId'] == run_id, 'driver-release-identity')
        return {'ok': True, 'runId': run_id, 'performanceAccepted': False}
    except BaseException as error:
        if not (evidence / 'workflow-result.json').exists():
            save(evidence / 'workflow-result.json', {'runId': run_id, 'ok': False,
                  'startedMonotonicNs': started, 'finishedMonotonicNs': ns(),
                  'error': error_message(error),
                  'performanceAccepted': False})
        raise
    finally:
        if task_dispatched and not success:
            # A foreground task CLI may have a detached owned worker. Cancel the same key.
            try:
                command(['/node/bin/node', '/source/dist/cli.js', '--root', '/evidence/tasks',
                         'task', 'cancel', '--request-id', run_id], budget['shutdownSeconds'],
                        budget['maxLogBytes'], env, record=evidence / 'http-cancel-command.json', check=False)
            except BaseException:
                pass  # Exiting container PID 1/init remains the independent final containment boundary.
        save(evidence / 'driver-active-usage.json', active_usage.audit)


def status(root):
    root, _, digest = load_attempt(root)
    value = {'attempt': str(root), 'runtimeDigest': digest, 'state': 'planned', 'performanceAccepted': False}
    for name in ('dispatch', 'current', 'result', 'stop-request'):
        if (root / (name + '.json')).exists():
            value[name] = read_json(root / (name + '.json'))
    if 'dispatch' in value:
        value['state'] = 'dispatch-recorded'
    if 'result' in value:
        value['state'] = 'completed' if value['result']['ok'] else 'failed'
    value['replayAllowed'] = False
    value['recovery'] = 'inspect same attempt/task; stop is safe; never dispatch run again'
    return value


def stop(root):
    controller = Controller(root)
    if not (controller.root / 'stop-request.json').exists():
        try:
            publish_marker(controller.root / 'stop-request.json',
                           {'runtimeDigest': controller.digest, 'atMonotonicNs': ns()})
        except FileExistsError:
            pass
    controller.cleanup = True
    errors = []
    registration = controller.root / 'execution/execution.json'
    if registration.exists():
        controller.execution = read_json(registration)
        for arm in controller.planned_arms():
            work = controller.root / 'work' / arm['runId']
            if not work.exists():
                continue
            # Stop request is durable even while Docker create has not returned. No repeated create/start.
            if (work / 'driver-dispatch.json').exists():
                try:
                    info = controller.inspect_driver(arm, missing=True)
                    if info and info['State']['Running']:
                        controller.docker('stop', '--time',
                                          str(controller.settings['driver']['shutdownSeconds']), info['Id'])
                except BaseException as error:
                    errors.append(str(error))
            if (work / 'environment/plan.json').exists():
                try:
                    controller.cli(['http', 'environment', 'stop', work / 'environment'], 90000)
                except BaseException as error:
                    errors.append(str(error))
    return {'stopRequested': True, 'errors': errors, 'performanceAccepted': False}


def describe():
    return {'schemaVersion': 1, 'kind': 'http-comparison-workflow-capability',
            'commands': ['freeze', 'run', 'status', 'stop'], 'runtimeSchema': 'http-comparison-runtime/v1',
            'effects': {'freeze': 'local snapshots only', 'run': 'single local Linux Docker dispatch',
                        'status': 'reads same attempt', 'stop': 'durable cancel and exact owned container stops'},
            'uses': ['http environment', 'hugegraph-mode-probe', 'http plan', 'task run/status/artifacts/cancel',
                     'resource-collector', 'paired-evidence'], 'retry': 'none', 'performanceAccepted': False,
            'limits': ['collector overhead uncalibrated', 'no measurement phase absolute clock anchors',
                       'no JVM metrics', 'no exclusive-host claim', 'no remote SSH execution'],
            'runtimeVersions': {'1': 'Node v1 profile; original contract', '2': 'Go v2 profile with hostIdentityGate; executor binary and clockProof required'},
            'goExecutorFields': ['engine', 'binary', 'clockProof'],
            'goLimits': ['exact Go source/binary SHA', 'read-only binary mount', 'same-binary/host clock proof', 'two held process observations per run/probe', 'collector/JVM fine attribution remains unqualified'],
            'runtimeRequiredFields': ['schemaVersion', 'kind', 'sourceRoot', 'nodeHome', 'hostPython',
                                      'dockerExecutable', 'driver', 'collector', 'supervisor'],
            'driverFields': ['image', 'python', 'cpus', 'memoryBytes', 'pids', 'tmpfsBytes', 'wallSeconds',
                             'shutdownSeconds', 'maxLogBytes'],
            'collectorFields': ['intervalMs', 'durationMs', 'maxSamples', 'maxOutputBytes'],
            'supervisorFields': ['pollMs', 'commandMs', 'maxCommandBytes', 'minFreeBytes', 'maxWorkBytes'],
            'exitCodes': {'0': 'command completed; inspect evidenceValid and statisticsEligible',
                          '6': 'failed or invalid; same attempt inspection only', '2': 'argparse input error'},
            'documentation': 'docs/http-comparison-run.md'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    frozen = commands.add_parser('freeze', help='Offline: bind comparison, code/runtime and driver/collector budgets')
    frozen.add_argument('--runtime', type=Path, required=True)
    frozen.add_argument('--plan', type=Path, required=True)
    frozen.add_argument('--out', type=Path, required=True, help='New directory; parent must exist')
    for action in ('run', 'status', 'stop'):
        child = commands.add_parser(action, help={'run': 'Once only; use task run for a detached controller',
                                                'status': 'Read same execution without dispatch',
                                                'stop': 'Cancel and stop exact owned containers'}[action])
        child.add_argument('--attempt', type=Path, required=True)
    commands.add_parser('describe', help='Machine readable contract and limitations')
    private = commands.add_parser('_driver', help=argparse.SUPPRESS)
    private.add_argument('--arm', choices=('A', 'B', 'O'), required=True)
    private.add_argument('--run-id', required=True)
    for child in commands.choices.values():
        child.add_argument('--json', action='store_true', help='JSON is always enabled')
    args = parser.parse_args()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal_cancel)
    try:
        if args.command == 'freeze':
            value = freeze(args.runtime, args.plan, args.out)
        elif args.command == 'run':
            value = Controller(args.attempt, verify=True).run()
        elif args.command == '_driver':
            value = driver(args.arm, args.run_id)
        else:
            value = {'status': status, 'stop': stop}.get(args.command, lambda _: describe())(
                getattr(args, 'attempt', None))
        print(json.dumps({'schemaVersion': 1, 'ok': value.get('ok', True), 'data': value}, allow_nan=False))
        return 0 if value.get('ok', True) and not value.get('errors') else 6
    except BaseException as error:
        code = error_message(error)
        if args.command == 'run' and args.attempt.is_dir() and not (args.attempt / 'result.json').exists():
            try:
                save(args.attempt / 'result.json', {'kind': 'http-comparison-workflow-result', 'ok': False,
                     'failure': code, 'executionCompleted': False, 'evidenceValid': False,
                     'finishedMonotonicNs': ns(), 'performanceAccepted': False})
            except OSError:
                pass
        print(json.dumps({'schemaVersion': 1, 'ok': False, 'error': {'code': code, 'retryable': False},
                          'performanceAccepted': False}))
        return 6


if __name__ == '__main__':
    sys.exit(main())
