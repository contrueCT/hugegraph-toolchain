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
"""Finite fixed-provider HTTP generator study. Never starts or connects to HugeGraph."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GO = load_module('generator_go', 'go-comparison-runtime.py')
CW = load_module('generator_resource', 'comparison-workflow.py')


def need(value, message):
    if not value:
        raise ValueError('GENERATOR: ' + message)


def read(path):
    return json.loads(Path(path).read_bytes())


def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write('\n')


def sha(path):
    with Path(path).open('rb') as stream:
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1048576), b''):
            digest.update(chunk)
        return digest.hexdigest()


def ns():
    return str(time.monotonic_ns())


def validate_inventory(root, files):
    seen = set()
    for entry in files:
        relative = entry['path']
        need(relative not in seen and not Path(relative).is_absolute() and '..' not in Path(relative).parts,
             'inventory-path')
        seen.add(relative)
        path = Path(root) / relative
        need(path.is_file() and not path.is_symlink() and path.stat().st_size == entry['bytes'] and
             sha(path) == entry['sha256'], 'inventory-changed: ' + relative)


class Study:
    def __init__(self, plan):
        self.root = Path(plan).resolve()
        self.plan = read(self.root / 'plan.json')
        need(sha(self.root / 'plan.json') == read(self.root / 'plan.lock.json')['planSha256'], 'plan-lock')
        self.request = self.plan['request']
        self.runtime = self.request['runtime']
        self.source = Path(self.plan['sourceRoot'])
        self.budget = self.request['budget']
        self.started = time.monotonic()
        self.end = self.started + self.budget['wallMs'] / 1000
        self.cancelled = False
        self.children = []
        self.owned = []
        self.created = []
        self.rows = []
        self.error = None
        self.observers = []
        self.out = self.root / 'output'
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.owner = uuid.uuid4().hex
        self.last_scan = float('-inf')
        self.evidence_size = 0
        self.node = '/node/bin/node'
        self.processes = []

    def guard(self):
        need(not self.cancelled, 'cancelled')
        need(time.monotonic() < self.end - self.budget['cleanupReserveMs'] / 1000, 'task-hard-deadline')
        if time.monotonic() - self.last_scan >= 2:
            self.last_scan = time.monotonic()
            need(shutil.disk_usage(self.root).free >= self.budget['minFreeBytes'], 'disk-floor')
            self.evidence_size = sum(p.stat().st_size for p in self.out.rglob('*') if p.is_file())
            need(self.evidence_size <= self.budget['maxEvidenceBytes'] - 1048576, 'evidence-cap')

    def command(self, argv, timeout=30, cleanup=False):
        if not cleanup:
            self.guard()
        result = subprocess.run([str(a) for a in argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=min(timeout, max(0.1, self.end - time.monotonic())), check=False)
        need(len(result.stdout) + len(result.stderr) <= 2097152, 'command-output-cap')
        need(result.returncode == 0, 'command-failed: ' + result.stderr.decode(errors='replace')[:1500])
        return result.stdout.decode()

    def docker(self, args, cleanup=False):
        return self.command([self.runtime['docker'], *args], cleanup=cleanup)

    def inspect(self, identifier, cleanup=False):
        return json.loads(self.docker(['inspect', identifier], cleanup=cleanup))[0]

    def create(self, name, cpus, memory, mounts, argv, network='none'):
        # Exact predeclared name plus owner label: cleanup never targets discovered unrelated containers.
        need(all(',' not in str(x) and '\n' not in str(x) for pair in mounts for x in pair), 'mount-path')
        args = ['create', '--name', name, '--label', 'hgbench.generator.owner=' + self.owner,
                '--network', network, '--user', str(os.getuid()) + ':' + str(os.getgid()), '--cpus', str(cpus), '--memory', str(memory), '--memory-swap', str(memory),
                '--pids-limit', '128', '--read-only', '--tmpfs', '/tmp:rw,nosuid,nodev,size=67108864',
                '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--restart', 'no',
                '--stop-timeout', '10']
        for src, target in mounts:
            readonly = target.endswith(':ro')
            if readonly:
                target = target[:-3]
            args += ['--mount', f'type=bind,src={src},dst={target}' + (',readonly' if readonly else '')]
        args += ['--entrypoint', str(argv[0]), self.runtime['image'], *map(str, argv[1:])]
        # Persist the planned name before create. Uncertain create outcomes are resolved by same-name inspect.
        self.created.append(name)
        save(self.out / f'create-{name}.json', {'name': name, 'owner': self.owner, 'argv': args})
        identifier = self.docker(args).strip()
        need(len(identifier) == 64 and all(c in '0123456789abcdef' for c in identifier), 'container-id')
        self.owned.append(identifier)
        info = self.inspect(identifier)
        config = info['HostConfig']
        need(info['Image'] == self.runtime['image'] and config['NanoCpus'] == int(cpus * 1e9) and
             config['Memory'] == config['MemorySwap'] == memory and config['PidsLimit'] == 128 and
             info['Config']['User'] == str(os.getuid()) + ':' + str(os.getgid()) and
             info['Config']['Labels']['hgbench.generator.owner'] == self.owner, 'container-limits')
        self.docker(['start', identifier])
        info = self.inspect(identifier)
        need(info['State']['Running'], 'container-not-running')
        self.processes.append(CW.process_identity('container-init', info['State']['Pid']))
        return identifier

    def wait_file(self, path, seconds=20):
        end = min(self.end, time.monotonic() + seconds)
        while not path.exists():
            self.guard()
            need(time.monotonic() < end, 'ready-timeout')
            time.sleep(0.05)

    def start_collector(self, folder, provider, driver):
        provider_pid = self.inspect(provider)['State']['Pid']
        driver_pid = self.inspect(driver)['State']['Pid']
        spec = {'schemaVersion': 1, 'kind': 'http-resource-collection', 'intervalMs': 1000,
                'durationMs': min(86400000, int((self.end - time.monotonic()) * 1000)),
                'maxSamples': 30000, 'maxOutputBytes': 67108864, 'filesystemPath': str(self.root),
                'roles': [{'role': 'provider', 'hostPid': provider_pid}, {'role': 'driver', 'hostPid': driver_pid}],
                'controlledStop': {'path': str(folder / 'collector-stop.json'), 'token': uuid.uuid4().hex}}
        save(folder / 'collector-spec.json', spec)
        stream = (folder / 'collector.log').open('xb')
        child = subprocess.Popen([self.runtime['hostPython'], '-B', str(HERE / 'resource-collector.py'),
                                  'collect', '--spec', str(folder / 'collector-spec.json'),
                                  '--out', str(folder / 'collector'), '--json'], stdout=stream, stderr=subprocess.STDOUT)
        stream.close()
        self.children.append(child)
        end = time.monotonic() + 20
        while CW.first_sample(folder / 'collector/samples.ndjson') is None:
            self.guard()
            need(child.poll() is None and time.monotonic() < end, 'collector-not-ready')
            time.sleep(0.05)
        return child

    def finish_collector(self, child, folder, finished):
        spec = read(folder / 'collector-spec.json')
        marker = {'schemaVersion': 1, 'kind': 'http-resource-stop', 'token': spec['controlledStop']['token'],
                  'specSha256': sha(folder / 'collector/spec.json'), 'hostBootId': self.boot,
                  'workflowFinishedMonotonicNs': finished, 'stopRequestedMonotonicNs': ns()}
        CW.publish_marker(folder / 'collector-stop.json', marker)
        child.wait(timeout=12)
        need(child.returncode == 0, 'collector-stop-failed')
        CW.verify_collector(folder / 'collector')

    def profile(self, arm, item, encoding):
        return {'schemaVersion': 2, 'kind': 'http-benchmark-profile', 'name': 'generator-' + item['id'],
                'benchmark': {**self.plan['suite'], 'caseIds': [item['id']], 'coverage': 'subset', 'purpose': 'calibration'},
                'serviceChecks': True, 'endpoint': 'http://127.0.0.1:18080/gremlin', 'responseEncoding': encoding,
                'authorizationEnv': None, 'target': {'label': 'Fixed correct response provider',
                    'build': self.plan['suite']['bundleSha256'], 'mode': 'http-generator-calibration',
                    'environment': 'HTTP generator calibration only; no HugeGraph or server-capacity conclusion.', 'evidenceFiles': []},
                'cases': [item], 'loads': [{'id': 'open-80', 'model': 'open', 'ratePerSecond': 80,
                                         'maxInFlight': 64, 'maxStartDelayMs': 1000}], 'repeats': 1,
                **self.request['timing'], 'maxRequestsPerPhase': 4800, 'maxResponseBytes': 2097152,
                'maxTraceBytes': 16777216, 'executor': {'engine': 'go-http-v1', 'binary': self.request['binary'],
                    'gomaxprocs': 4, 'cpuQuotaCores': arm['cpus'], 'maxRssBytes': arm['memoryBytes'],
                    'maxPendingBytes': 1048576, 'hostIdentityGate': True}}

    def run_unit(self, arm, folder, driver, item, encoding):
        unit = folder / 'units' / (item['id'] + '-' + encoding)
        unit.mkdir()
        profile = self.profile(arm, item, encoding)
        save(unit / 'input.json', profile)
        inside = '/evidence/units/' + unit.name
        self.docker(['exec', driver, self.node, '/source/dist/cli.js', 'http', 'plan', inside + '/input.json',
                     '--out', inside + '/attempt'])
        workflow = [self.runtime['docker'], 'exec', driver, self.node,
                    '/source/components/http/workflow.mjs', '--plan', inside + '/attempt']
        log = (unit / 'workflow.log').open('xb')
        started = ns()
        child = subprocess.Popen(workflow, stdout=log, stderr=subprocess.STDOUT)
        log.close()
        self.children.append(child)
        observer = GO.ProcessObserver(profile, driver, unit / 'attempt/output', unit / 'host-processes',
                                      self.docker, self.guard)
        self.observers.append(observer)
        while child.poll() is None:
            self.guard()
            observer.poll()
            time.sleep(0.05)
        observer.poll()
        finished = ns()
        save(unit / 'workflow.json', {'startedMonotonicNs': started, 'finishedMonotonicNs': finished,
                                     'exitCode': child.returncode})
        need(child.returncode == 0, 'unit-workflow-failed')
        save(unit / 'host-summary.json', observer.finish())
        # Pressure stays visible. Corrupt evidence/wrong answers stop the study without replacement.
        self.docker(['exec', driver, self.node, '/source/components/http/analyzer.mjs', '--input',
                     inside + '/attempt/output', '--output', inside + '/analysis'])
        analysis = read(unit / 'analysis/report.json')
        need(not any(row['failed'] for row in analysis['runs']), 'request-error-or-wrong-answer')
        self.rows.append({'arm': arm['id'], 'caseId': item['id'], 'encoding': encoding,
                          'path': str(unit.relative_to(self.out)), 'state': 'completed'})
        print(json.dumps({'event': 'unit-completed', **self.rows[-1]}), flush=True)

    def run_arm(self, arm):
        if self.budget['mode'] == 'per-arm':
            self.end = time.monotonic() + self.budget['wallMs'] / 1000
        self.guard()
        folder = self.out / arm['id']
        folder.mkdir()
        (folder / 'units').mkdir()
        mounts = [(self.source, '/source:ro'), (self.runtime['nodeRoot'], '/node:ro'),
                  (self.root, '/study:ro'), (folder, '/evidence')]
        remaining = int((self.end - time.monotonic()) * 1000 - self.budget['cleanupReserveMs'])
        provider = self.create('hgbench-gen-' + self.owner[:12] + '-' + arm['id'] + '-p', 1, 536870912, mounts,
            [self.node, '/source/components/http/generator-provider.mjs', '--assets', '/study/assets',
             '--output', '/evidence/provider', '--wall-ms', min(86400000, remaining)])
        self.wait_file(folder / 'provider/ready.json')
        binary = self.request['binary']['path']
        mounts = [*mounts, (binary, binary + ':ro')]
        anchor = 'import signal,sys,time;signal.signal(signal.SIGTERM,lambda *a:sys.exit(0));time.sleep(' + str(remaining / 1000) + ')'
        driver = self.create('hgbench-gen-' + self.owner[:12] + '-' + arm['id'] + '-d', arm['cpus'],
                             arm['memoryBytes'], mounts, [self.runtime['containerPython'], '-c', anchor],
                             'container:' + provider)
        save(folder / 'containers-before.json', {'provider': self.inspect(provider), 'driver': self.inspect(driver)})
        ready = read(folder / 'provider/ready.json')
        provider_pid = self.inspect(provider)['State']['Pid']
        need(ready['pid'] == 1 and sha(Path('/proc') / str(provider_pid) / 'exe') == ready['executableSha256'],
             'provider-actual-executable')
        capture = CW.process_identity('provider', provider_pid)
        capture['executableSha256'] = sha(Path('/proc') / str(provider_pid) / 'exe')
        capture['readySha256'] = sha(folder / 'provider/ready.json')
        save(folder / 'provider-host-process.json', capture)
        collector = self.start_collector(folder, provider, driver)
        started = ns()
        for item in self.plan['cases']:
            for encoding in self.plan['encodings']:
                self.run_unit(arm, folder, driver, item, encoding)
        finished = ns()
        self.finish_collector(collector, folder, finished)
        save(folder / 'window.json', {'startedMonotonicNs': started, 'finishedMonotonicNs': finished})
        save(folder / 'containers-after.json', {'provider': self.inspect(provider), 'driver': self.inspect(driver)})
        self.stop_container(driver)
        self.stop_container(provider)
        need((folder / 'provider/summary.json').is_file(), 'provider-terminal-missing')

    def stop_container(self, identifier):
        info = self.inspect(identifier, cleanup=True)
        need(info['Config']['Labels'].get('hgbench.generator.owner') == self.owner, 'cleanup-ownership')
        if info['State']['Running']:
            self.docker(['stop', '-t', '10', identifier], cleanup=True)
        info = self.inspect(identifier, cleanup=True)
        save(self.out / ('stopped-' + identifier + '.json'), info)
        self.docker(['rm', identifier], cleanup=True)
        self.owned.remove(identifier)

    def run(self):
        validate_inventory(self.source, self.plan['sourceInventory'])
        validate_inventory(self.root, self.plan['assetsInventory'])
        for entry in self.plan['runtimeFiles']:
            need(Path(entry['path']).stat().st_size == entry['bytes'] and sha(entry['path']) == entry['sha256'], 'runtime-executable-drift')
        self.out.mkdir()  # create-only dispatch marker, including failed and cancelled attempts
        save(self.out / 'execution.json', {'schemaVersion': 1, 'kind': 'http-generator-calibration-execution',
            'owner': self.owner, 'environmentKind': 'http-generator-calibration', 'hostBootId': self.boot,
            'startedMonotonicNs': ns(), 'planSha256': sha(self.root / 'plan.json'), 'performanceAccepted': False})
        try:
            self.guard()
            need(sys.platform == 'linux' and os.cpu_count() >= 8 and os.getuid() > 0 and os.getgid() > 0, 'nonroot-linux-eight-cpu-host-required')
            need(self.plan['user'] == {'uid': os.getuid(), 'gid': os.getgid()}, 'frozen-user-drift')
            need(sha(self.request['binary']['path']) == self.request['binary']['sha256'], 'binary-file-sha')
            need(self.command([self.runtime['nodeRoot'] + '/bin/node', '--version']).strip().startswith('v22.'), 'node22')
            # Exact same-binary proof is copied, then independently read by the frozen installed reader.
            shutil.copytree(self.runtime['clockProof'], self.out / 'clock-proof')
            self.command([self.runtime['nodeRoot'] + '/bin/node', self.source / 'dist/cli.js', 'http', 'engine',
                          'clock-verify', str(self.out / 'clock-proof')])
            proof = read(self.out / 'clock-proof/proof.json')
            need(proof['imageId'] == self.runtime['image'] and proof['binarySha256'] == self.request['binary']['sha256'] and proof['host']['bootId'] == self.boot, 'clock-binary-image-boot')
            save(self.out / 'host.json', {'cpuinfo': Path('/proc/cpuinfo').read_text(),
                'meminfo': Path('/proc/meminfo').read_text(), 'swaps': Path('/proc/swaps').read_text(),
                'dockerPs': self.docker(['ps', '--no-trunc', '--format', '{{json .}}']),
                'uname': list(os.uname()), 'virtualization': self.command(['/usr/bin/systemd-detect-virt'])})
            for arm in self.plan['arms']:
                validate_inventory(self.source, self.plan['sourceInventory'])
                self.run_arm(arm)
        except BaseException as error:
            self.error = type(error).__name__ + ': ' + str(error)[:2000]
        finally:
            # Cleanup gets its reserved interval even after the active deadline/cancellation.
            self.end = max(self.end, time.monotonic() + self.budget['cleanupReserveMs'] / 1000)
            cleanup_errors = []
            for child in self.children:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=3)
            # Resolve any create call whose success response was lost, by exact frozen name and owner label.
            for name in self.created:
                try:
                    result = subprocess.run([self.runtime['docker'], 'inspect', name], capture_output=True, timeout=10)
                    if result.returncode == 0:
                        info = json.loads(result.stdout)[0]
                        need(info['Config']['Labels'].get('hgbench.generator.owner') == self.owner, 'cleanup-name-owner')
                        if info['Id'] not in self.owned:
                            self.owned.append(info['Id'])
                    else:
                        need('no such object' in result.stderr.decode().lower(), 'cleanup-inspect-unknown')
                except Exception as error:
                    cleanup_errors.append(str(error))
            for identifier in list(reversed(self.owned)):
                try:
                    self.stop_container(identifier)
                except Exception as error:
                    cleanup_errors.append(str(error))
            for process in self.processes:
                proc = Path('/proc') / str(process['hostPid']) / 'stat'
                if proc.exists() and proc.read_text().rsplit(')', 1)[1].split()[19] == process['statRaw'].rsplit(')', 1)[1].split()[19]:
                    cleanup_errors.append('container-init-still-live:' + str(process['hostPid']))
            observed_go = sorted({identity for observer in self.observers for identity in observer.host_processes})
            for pid, start in observed_go:
                proc = Path('/proc') / str(pid) / 'stat'
                if proc.exists() and proc.read_text().rsplit(')', 1)[1].split()[19] == start:
                    cleanup_errors.append('observed-go-still-live:' + str(pid))
            save(self.out / 'cleanup.json', {'createdNames': self.created, 'remainingOwned': self.owned,
                'observedGoProcesses': [{'hostPid': pid, 'startTicks': start} for pid, start in observed_go],
                'capturedProcesses': self.processes, 'errors': cleanup_errors, 'performanceAccepted': False})
            complete = len(self.rows) == 88 and self.error is None and not cleanup_errors
            save(self.out / 'result.json', {'schemaVersion': 1, 'kind': 'http-generator-calibration-result',
                'state': 'completed' if complete else 'partial', 'error': self.error, 'completedUnits': self.rows,
                'plannedUnits': 88, 'uncompletedUnits': [
                    {'arm': arm['id'], 'caseId': case['id'], 'encoding': encoding} for arm in self.plan['arms']
                    for case in self.plan['cases'] for encoding in self.plan['encodings'] if not any(
                    row['arm'] == arm['id'] and row['caseId'] == case['id'] and row['encoding'] == encoding for row in self.rows)],
                'performanceAccepted': False, 'finishedMonotonicNs': ns()})
            files = []
            for path in sorted(self.out.rglob('*')):
                if path.is_file():
                    files.append({'path': str(path.relative_to(self.out)), 'bytes': path.stat().st_size, 'sha256': sha(path)})
            save(self.out / 'delivery.lock.json', {'schemaVersion': 1, 'kind': 'http-generator-calibration-evidence',
                'files': files, 'performanceAccepted': False})
        return complete


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True, help='Frozen new study plan; existing output is never replayed')
    args = parser.parse_args()
    study = Study(args.plan)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: setattr(study, 'cancelled', True))
    return 0 if study.run() else 6


if __name__ == '__main__':
    sys.exit(main())
