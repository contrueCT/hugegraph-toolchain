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

"""Sequential, once-only A/A cohort orchestration through existing comparison tasks.

No statistical implementation, target reset policy or automatic retry. Preparation
is offline. run requires recorded inactive retained SF100 jobs before dispatch.
An interrupted cohort can be inspected/stopped; its execution is never restarted.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import sys
import time
import uuid

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location('hgbench_comparison_workflow',
                                             Path(__file__).with_name('comparison-workflow.py'))
COMMON = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMMON)
MiB = 1024 ** 2
CANCELLED = False
CONTROLS = ('calibration-workflow.py', 'comparison-workflow.py', 'comparison-runtime.json')
need, read_json, save = COMMON.need, COMMON.read_json, COMMON.save
entry, read_bytes, exact, integer = COMMON.file_entry, COMMON.read_bytes, COMMON.exact, COMMON.integer


def control_files(runtime):
    return CONTROLS + COMMON.engine_control_files(runtime['comparisonRuntime'])


def cancel(_signal, _frame):
    global CANCELLED
    CANCELLED = True


def validate_runtime(value):
    exact(value, ('schemaVersion', 'kind', 'comparisonRuntime', 'cleanWindow', 'budgets'), 'cohort-runtime-fields')
    need(value['schemaVersion'] == 1 and value['kind'] == 'http-calibration-runtime', 'cohort-runtime-identity')
    COMMON.validate_runtime(value['comparisonRuntime'])
    exact(value['cleanWindow'], ('systemctlExecutable', 'requireClean', 'units'), 'cohort-clean-window-fields')
    COMMON.path_value(value['cleanWindow']['systemctlExecutable'])
    need(value['cleanWindow']['requireClean'] is True, 'cohort-clean-window-required')
    units = value['cleanWindow']['units']
    need(isinstance(units, list) and 1 <= len(units) <= 32 and
         all(isinstance(unit, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}\.service', unit)
             for unit in units) and len(units) == len(set(units)), 'cohort-clean-window-units')
    budgets = value['budgets']
    exact(budgets, ('prepareMilliseconds', 'maxRunMilliseconds', 'pollMilliseconds', 'commandMilliseconds',
                    'maxCommandBytes', 'maxWorkBytes', 'minFreeBytes', 'cleanupMilliseconds'), 'cohort-budget-fields')
    for key, low, high in [('prepareMilliseconds', 1000, 7 * 86400000),
                           ('maxRunMilliseconds', 1000, 7 * 86400000), ('pollMilliseconds', 1000, 60000),
                           ('commandMilliseconds', 1000, 300000), ('maxCommandBytes', 65536, 16 * MiB),
                           ('maxWorkBytes', MiB, 1024 ** 4), ('minFreeBytes', 0, 1024 ** 5),
                           ('cleanupMilliseconds', 5000, 20000)]:
        integer(budgets[key], low, high, 'cohort-' + key)
    need(budgets['maxRunMilliseconds'] + budgets['cleanupMilliseconds'] + 10000 <= 7 * 86400000,
         'cohort-task-seven-day-budget')


def task_id(request_id):
    need(isinstance(request_id, str) and re.fullmatch(r'[\x21-\x7e]{1,128}', request_id),
         'cohort-task-request-id')
    # src/tasks.ts taskRequestId hashes src/io.ts hash(key): JSON.stringify of
    # the string, including JSON quotes/escapes, encoded as UTF-8.
    encoded = json.dumps(request_id, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return 'task-' + hashlib.sha256(encoded).hexdigest()[:32]


def cli_argv(runtime, *args):
    shared = runtime['comparisonRuntime']
    return [str(Path(shared['nodeHome']) / 'bin/node'), str(Path(shared['sourceRoot']) / 'dist/cli.js'),
            *map(str, args)]


def invoke(runtime, args, seconds=None, check=lambda: None, record=None):
    budget = runtime['budgets']
    raw = COMMON.command(cli_argv(runtime, *args), seconds or budget['commandMilliseconds'] / 1000,
                         budget['maxCommandBytes'], COMMON.clean_env(runtime['comparisonRuntime']['hostPython'],
                         runtime['comparisonRuntime']['nodeHome']), check, record, False)
    return COMMON.response(raw)


def freeze(runtime_file, calibration, output):
    started = time.monotonic()
    runtime = read_json(runtime_file)
    validate_runtime(runtime)
    need(os.geteuid() != 0, 'cohort-nonroot-owner-required')
    root = Path(output).resolve()
    need(not root.exists(), 'cohort-attempt-already-exists')
    source = Path(calibration).resolve(strict=True)
    need(source != root and source not in root.parents and root not in source.parents, 'cohort-overlaps-calibration')
    usage = None
    last_scan = [0.0]
    def check():
        need(not CANCELLED and time.monotonic() - started < runtime['budgets']['prepareMilliseconds'] / 1000,
             'cohort-prepare-deadline-or-cancelled')
        if usage is not None and time.monotonic() - last_scan[0] >= 1:
            last_scan[0] = time.monotonic()
            usage.sample()
    verified = invoke(runtime, ['http', 'calibration', 'verify', source], 330, check)
    root.mkdir(mode=0o700)
    usage = COMMON.ActiveUsage(root, runtime['budgets']['maxWorkBytes'], 'cohort-prepare-work-budget',
                               runtime['budgets']['minFreeBytes'])
    COMMON.copy_regular_tree(source, root / 'calibration', 512 * MiB, check)
    (root / 'children').mkdir()
    (root / 'prepare-commands').mkdir()
    save(root / 'comparison-runtime.json', runtime['comparisonRuntime'])
    attempt_id = uuid.uuid4().hex
    children = []
    maximum = runtime['budgets']['cleanupMilliseconds'] + 60000
    for child in verified['experiments']:
        check()
        destination = root / 'children' / child['id']
        prepared = invoke(runtime, ['http', 'comparison', 'prepare', root / 'comparison-runtime.json',
                                  '--plan', root / 'calibration' / child['directory'], '--out', destination],
                          min(330, runtime['budgets']['prepareMilliseconds'] / 1000 - (time.monotonic() - started)),
                          check, root / 'prepare-commands' / (child['id'] + '.json'))
        need(prepared['attempt'] == str(destination) and prepared['executed'] is False,
             'cohort-child-preparation-identity')
        task = read_json(prepared['taskPlan'])
        request_id = 'calibration:' + attempt_id + ':' + child['id']
        children.append({'id': child['id'], 'planDigest': child['planDigest'], 'directory': str(destination),
                         'taskPlan': str(Path(prepared['taskPlan']).resolve(strict=True)),
                         'taskPlanDigest': task['digest'], 'taskPlanSha256': entry(prepared['taskPlan'])['sha256'],
                         'requestId': request_id, 'taskId': task_id(request_id)})
        maximum += task['plan']['timeoutMs'] + 60000
    need(maximum <= runtime['budgets']['maxRunMilliseconds'], 'cohort-total-task-budget')
    for name in ('calibration-workflow.py', 'comparison-workflow.py',
                 *COMMON.engine_control_files(runtime['comparisonRuntime'])):
        shutil.copyfile(Path(__file__).with_name(name), root / name)
    controls = [entry(root / name, name) for name in control_files(runtime)]
    plan = {'schemaVersion': 1, 'kind': 'http-calibration-workflow-plan', 'attemptId': attempt_id,
            'root': str(root), 'owner': {'uid': os.geteuid(), 'gid': os.getegid()}, 'runtime': runtime,
            'calibrationPlanDigest': verified['planDigest'], 'calibrationContentDigest': verified['contentDigest'],
            'children': children, 'taskRoot': str(root / 'task-state'),
            'requiredJobs': runtime['cleanWindow']['units'],
            'controlFiles': controls, 'budgetSemantics': 'bounded CLI/subtasks; sampled work bytes; no kernel I/O guarantee',
            'qualificationAccepted': False, 'performanceAccepted': False}
    save(root / 'workflow.json', plan)
    save(root / 'workflow.lock.json', {'schemaVersion': 1, 'kind': 'http-calibration-workflow-lock',
                                     'attemptId': attempt_id, 'planSha256': entry(root / 'workflow.json')['sha256']})
    return {'attempt': str(root), 'planSha256': entry(root / 'workflow.json')['sha256'], 'executed': False,
            'maxRunMilliseconds': runtime['budgets']['maxRunMilliseconds'],
            'cleanupMilliseconds': runtime['budgets']['cleanupMilliseconds'], 'children': children,
            'hostPython': runtime['comparisonRuntime']['hostPython'],
            'qualificationAccepted': False, 'performanceAccepted': False}


def load(root, verify=False):
    root = Path(root).resolve(strict=True)
    plan, lock = read_json(root / 'workflow.json'), read_json(root / 'workflow.lock.json')
    exact(plan, ('schemaVersion', 'kind', 'attemptId', 'root', 'owner', 'runtime', 'calibrationPlanDigest',
                 'calibrationContentDigest', 'children', 'taskRoot', 'requiredJobs', 'controlFiles',
                 'budgetSemantics', 'qualificationAccepted', 'performanceAccepted'), 'cohort-plan-fields')
    exact(lock, ('schemaVersion', 'kind', 'attemptId', 'planSha256'), 'cohort-plan-lock-fields')
    need(plan['schemaVersion'] == 1 and plan['kind'] == 'http-calibration-workflow-plan' and
         lock['schemaVersion'] == 1 and lock['kind'] == 'http-calibration-workflow-lock' and
         plan['qualificationAccepted'] is False and plan['performanceAccepted'] is False and
         plan['root'] == str(root) and
         re.fullmatch(r'[a-f0-9]{32}', plan['attemptId']) and lock['attemptId'] == plan['attemptId'] and
         lock['planSha256'] == entry(root / 'workflow.json')['sha256'], 'cohort-plan-identity')
    need(plan['owner'] == {'uid': os.geteuid(), 'gid': os.getegid()} and os.geteuid() != 0, 'cohort-owner-mismatch')
    validate_runtime(plan['runtime'])
    need(plan['requiredJobs'] == plan['runtime']['cleanWindow']['units'], 'cohort-job-set-drift')
    need(plan['taskRoot'] == str(root / 'task-state'), 'cohort-task-root')
    need([f['path'] for f in plan['controlFiles']] == list(control_files(plan['runtime'])), 'cohort-control-inventory')
    ids = set()
    maximum = plan['runtime']['budgets']['cleanupMilliseconds'] + 60000
    for child in plan['children']:
        exact(child, ('id', 'planDigest', 'directory', 'taskPlan', 'taskPlanDigest', 'taskPlanSha256',
                      'requestId', 'taskId'), 'cohort-child-fields')
        need(child['id'] not in ids and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', child['id']) and
             child['directory'] == str(root / 'children' / child['id']) and
             child['taskPlan'] == str(root / 'children' / child['id'] / 'task.json') and
             child['requestId'] == 'calibration:' + plan['attemptId'] + ':' + child['id'] and
             child['taskId'] == task_id(child['requestId']), 'cohort-child-identity')
        ids.add(child['id'])
        for key in ('planDigest', 'taskPlanDigest', 'taskPlanSha256'):
            need(isinstance(child[key], str) and re.fullmatch('[a-f0-9]{64}', child[key]), 'cohort-child-digest')
        task = read_json(child['taskPlan'])
        need(entry(child['taskPlan'])['sha256'] == child['taskPlanSha256'] and
             task['digest'] == child['taskPlanDigest'], 'cohort-child-task-changed')
        integer(task['plan']['timeoutMs'], 100, 7 * 86400000, 'cohort-child-task-timeout')
        maximum += task['plan']['timeoutMs'] + 60000
    need(1 <= len(ids) <= 100, 'cohort-child-count')
    need(maximum <= plan['runtime']['budgets']['maxRunMilliseconds'], 'cohort-total-task-budget')
    if verify:
        for item in plan['controlFiles']:
            need(entry(root / item['path'], item['path']) == item, 'cohort-control-changed')
        need(entry(__file__)['sha256'] == next(f['sha256'] for f in plan['controlFiles']
             if f['path'] == 'calibration-workflow.py'), 'execute-frozen-cohort-controller')
    return root, plan, lock['planSha256']


class Controller:
    def __init__(self, attempt):
        self.root, self.plan, self.sha = load(attempt)
        self.runtime = self.plan['runtime']
        self.started = time.monotonic()
        self.cleanup = False
        self.cleanup_deadline = None
        self.active_usage = COMMON.ActiveUsage(self.root, self.runtime['budgets']['maxWorkBytes'],
                                               'cohort-work-budget', self.runtime['budgets']['minFreeBytes'])
        self.last_scan = 0

    def stopping(self):
        marker = self.root / 'stop-request.json'
        if marker.exists():
            value = read_json(marker)
            need(value == {'attemptId': self.plan['attemptId'], 'planSha256': self.sha}, 'cohort-stop-identity')
            return True
        return CANCELLED

    def check(self):
        if self.cleanup:
            return
        need(not self.stopping(), 'cohort-stop-requested')
        need(time.monotonic() - self.started < self.runtime['budgets']['maxRunMilliseconds'] / 1000,
             'cohort-run-deadline')
        if time.monotonic() - self.last_scan >= 1:
            self.last_scan = time.monotonic()
            self.active_usage.sample()

    def cli(self, args, seconds=None, record=None):
        if self.cleanup_deadline is not None:
            left = self.cleanup_deadline - time.monotonic()
            need(left > 0, 'cohort-cleanup-deadline')
            seconds = min(seconds or self.runtime['budgets']['commandMilliseconds'] / 1000, left)
        return invoke(self.runtime, args, seconds, self.check, record)

    def pause(self, seconds):
        # Signals only flip a flag. Short slices also observe durable stop markers
        # without increasing CLI/task polling frequency or delaying cleanup past parent grace.
        end = time.monotonic() + seconds
        while True:
            self.check()
            left = end - time.monotonic()
            if left <= 0:
                return
            time.sleep(min(0.25, left))

    def snapshot(self, child, seconds=None):
        value = self.cli(['--root', self.plan['taskRoot'], 'task', 'status', '--request-id', child['requestId']], seconds)
        need(value['taskId'] == child['taskId'] and value['planDigest'] == child['taskPlanDigest'] and
             type(value['terminal']) is bool and type(value['interrupted']) is bool, 'cohort-child-task-identity')
        return value

    def clean_window(self, name):
        rows = []
        clean = True
        for unit in self.plan['requiredJobs']:
            try:
                env = COMMON.clean_env(self.runtime['comparisonRuntime']['hostPython'])
                env.update(XDG_RUNTIME_DIR='/run/user/' + str(os.geteuid()),
                           DBUS_SESSION_BUS_ADDRESS='unix:path=/run/user/' + str(os.geteuid()) + '/bus')
                result = COMMON.command([self.runtime['cleanWindow']['systemctlExecutable'], '--user', 'show', unit,
                    '-p', 'Id', '-p', 'LoadState', '-p', 'ActiveState', '-p', 'SubState', '-p', 'MainPID',
                    '-p', 'ControlGroup', '-p', 'Result'], 10, 65536,
                    env, self.check, None, False)
                pairs = [line.split('=', 1) for line in result['stdout'].splitlines() if '=' in line]
                fields = dict(pairs)
                passed = (result['exitCode'] == 0 and fields.get('Id') == unit and
                          fields.get('LoadState') in ('loaded', 'not-found') and
                          fields.get('ActiveState') == 'inactive' and fields.get('MainPID') == '0' and
                          fields.get('SubState') == 'dead' and not fields.get('ControlGroup') and
                          len(pairs) == 7 and set(fields) == {'Id', 'LoadState', 'ActiveState', 'SubState',
                                                           'MainPID', 'ControlGroup', 'Result'})
                state_evidence = ('unit-absent' if fields.get('LoadState') == 'not-found'
                                  else 'loaded-inactive') if passed else 'unproven'
                rows.append({'unit': unit, 'raw': result, 'fields': fields,
                             'stateEvidence': state_evidence, 'inactiveObserved': passed})
                clean &= passed
            except Exception as error:
                rows.append({'unit': unit, 'error': str(error),
                             'stateEvidence': 'unproven', 'inactiveObserved': False})
                clean = False
        receipt = {'schemaVersion': 1, 'kind': 'http-calibration-clean-window-check',
                   'planSha256': self.sha, 'observedMonotonicNs': COMMON.ns(), 'jobs': rows,
                   'dispatchAllowed': clean, 'wholeWindowExclusivityProven': False, 'performanceAccepted': False}
        save(self.root / 'observations' / (name + '.json'), receipt)
        need(clean, 'cohort-clean-window-not-proven')
        return receipt

    def stop_child(self, child):
        errors = []
        end = time.monotonic() + self.runtime['budgets']['cleanupMilliseconds'] / 1000
        self.cleanup_deadline = end
        label_id = uuid.uuid4().hex
        (self.root / 'observations').mkdir(exist_ok=True)
        try:
            observed = self.snapshot(child, 1)
            if observed['terminal'] and not observed['interrupted']:
                return {'task': observed, 'errors': errors, 'taskTerminalProven': True}
        except Exception as error:
            errors.append(str(error))
        for label, args in [('comparison-stop', ['http', 'comparison', 'stop', child['directory']]),
                            ('task-cancel', ['--root', self.plan['taskRoot'], 'task', 'cancel', '--id', child['taskId']])]:
            try:
                self.cli(args, self.runtime['budgets']['cleanupMilliseconds'] / 3000,
                         self.root / 'observations' / (child['id'] + '-' + label + '-' + label_id + '.json'))
            except Exception as error:
                errors.append(str(error))
        while time.monotonic() < end:
            try:
                observed = self.snapshot(child)
                if observed['terminal'] and not observed['interrupted']:
                    return {'task': observed, 'errors': errors, 'taskTerminalProven': True}
            except Exception as error:
                errors.append(str(error))
                errors = errors[-10:]
            time.sleep(min(self.runtime['budgets']['pollMilliseconds'] / 1000, max(0, end - time.monotonic())))
        errors.append('cohort-child-cleanup-terminal-unproven')
        return {'errors': errors[-10:], 'taskTerminalProven': False}

    def run(self):
        need(platform.system() == 'Linux' and os.geteuid() != 0, 'cohort-run-nonroot-linux-required')
        load(self.root, verify=True)
        save(self.root / 'dispatch.json', {'attemptId': self.plan['attemptId'], 'planSha256': self.sha,
                                         'pid': os.getpid(), 'startedMonotonicNs': COMMON.ns()})
        (self.root / 'observations').mkdir()
        rows = [{'id': child['id'], 'state': 'not-started', 'taskId': child['taskId']} for child in self.plan['children']]
        active = None
        failure = None
        cleanup = None
        try:
            self.check()
            verified = self.cli(['http', 'calibration', 'verify', self.root / 'calibration'])
            need(verified['planDigest'] == self.plan['calibrationPlanDigest'] and
                 verified['contentDigest'] == self.plan['calibrationContentDigest'], 'cohort-calibration-drift')
            need([(c['id'], c['planDigest']) for c in verified['experiments']] ==
                 [(c['id'], c['planDigest']) for c in self.plan['children']], 'cohort-child-plan-order-drift')
            for child, row in zip(self.plan['children'], rows):
                self.check()
                self.clean_window(child['id'] + '-before')
                active = child
                row['state'] = 'dispatch-recorded'
                save(self.root / 'current.json', {'id': child['id'], 'planSha256': self.sha}, replace=True)
                save(self.root / 'observations' / (child['id'] + '-dispatch.json'),
                     {'id': child['id'], 'requestId': child['requestId'], 'taskId': child['taskId'],
                      'planSha256': self.sha, 'atMonotonicNs': COMMON.ns()})
                try:
                    submitted = self.cli(['--root', self.plan['taskRoot'], 'task', 'run', '--plan', child['taskPlan'],
                                          '--detach', '--request-id', child['requestId']], record=
                                         self.root / 'observations' / (child['id'] + '-submission.json'))
                    need(submitted['taskId'] == child['taskId'] and submitted.get('reused') is not True,
                         'cohort-child-already-submitted')
                except Exception:
                    row['state'] = 'submission-response-unknown'
                    # A committed submission can outlive a lost response. Query only; never submit twice.
                    try:
                        observed = self.snapshot(child)
                        save(self.root / 'observations' / (child['id'] + '-unknown-submission-status.json'), observed)
                    except Exception as status_error:
                        save(self.root / 'observations' / (child['id'] + '-unknown-submission-status.json'),
                             {'unavailable': True, 'error': str(status_error)[:2048]})
                    raise
                row['state'] = 'running'
                while True:
                    self.check()
                    observed = self.snapshot(child)
                    need(observed['taskId'] == child['taskId'] and observed['planDigest'] == child['taskPlanDigest'],
                         'cohort-child-task-identity')
                    if observed['terminal']:
                        save(self.root / 'observations' / (child['id'] + '-terminal.json'), observed)
                        row.update(state=observed['state'], interrupted=observed['interrupted'])
                        need(observed['state'] == 'completed' and not observed['interrupted'], 'cohort-child-failed')
                        break
                    self.pause(self.runtime['budgets']['pollMilliseconds'] / 1000)
                active = None
                self.clean_window(child['id'] + '-after')
        except BaseException as error:
            failure = str(error) if isinstance(error, COMMON.WorkflowError) else type(error).__name__
        finally:
            self.cleanup = True
            if active is not None:
                cleanup = self.stop_child(active)
            mappings = [{'id': child['id'], 'executionDirectory': 'children/' + child['id'] + '/execution'}
                        for child in self.plan['children'] if (Path(child['directory']) / 'execution').is_dir()
                        and not (Path(child['directory']) / 'execution').is_symlink()]
            save(self.root / 'execution-set.json', {'schemaVersion': 1, 'kind': 'http-calibration-execution-set',
                'planDigest': self.plan['calibrationPlanDigest'], 'experiments': mappings})
            result = {'schemaVersion': 1, 'kind': 'http-calibration-workflow-result', 'attemptId': self.plan['attemptId'],
                      'planSha256': self.sha, 'ok': failure is None, 'failure': failure, 'cleanup': cleanup,
                      'experiments': rows, 'finishedMonotonicNs': COMMON.ns(),
                      'wholeWindowExclusivityProven': False, 'qualificationAccepted': False, 'performanceAccepted': False}
            save(self.root / 'result.json', result)
            save(self.root / 'active-usage.json', self.active_usage.audit)
            save(self.root / 'evidence.lock.json', {'schemaVersion': 1,
                 'kind': 'http-calibration-workflow-summary-lock', 'attemptId': self.plan['attemptId'],
                 'planSha256': self.sha, 'files': [entry(self.root / name, name) for name in
                     ('dispatch.json', 'execution-set.json', 'result.json', 'active-usage.json')],
                 'scope': 'cohort summaries only; child executions require independent calibration report verification'})
        return result


def status(attempt, read_task=True):
    controller = Controller(attempt)
    controller.cleanup = True  # Read the same task even after a durable stop or a controller interruption.
    data = {'attempt': str(controller.root), 'attemptId': controller.plan['attemptId'],
            'planSha256': controller.sha, 'state': 'planned', 'replayAllowed': False,
            'recovery': 'inspect/stop same cohort and task; do not run this cohort again',
            'qualificationAccepted': False, 'performanceAccepted': False}
    if (controller.root / 'dispatch.json').exists():
        data['state'] = 'dispatch-recorded'
    if (controller.root / 'result.json').exists():
        data.update(state='terminal', result=read_json(controller.root / 'result.json'))
    if (controller.root / 'current.json').exists():
        current = read_json(controller.root / 'current.json')
        need(current['planSha256'] == controller.sha, 'cohort-current-identity')
        child = next(c for c in controller.plan['children'] if c['id'] == current['id'])
        if read_task:
            try:
                data['activeTask'] = controller.snapshot(child)
            except Exception as error:
                data['activeTask'] = {'unavailable': True, 'error': str(error)}
    return data


def stop(attempt):
    controller = Controller(attempt)
    marker = controller.root / 'stop-request.json'
    if not marker.exists():
        temporary = controller.root / ('stop-' + uuid.uuid4().hex + '.tmp')
        save(temporary, {'attemptId': controller.plan['attemptId'], 'planSha256': controller.sha})
        try:
            try:
                os.link(temporary, marker)
            except FileExistsError:
                pass
        finally:
            temporary.unlink()
    controller.stopping()
    if (controller.root / 'current.json').exists():
        current = read_json(controller.root / 'current.json')
        need(current['planSha256'] == controller.sha, 'cohort-current-identity')
        child = next(c for c in controller.plan['children'] if c['id'] == current['id'])
        controller.cleanup = True
        stopped = controller.stop_child(child)
    else:
        stopped = None
    value = status(attempt, read_task=False)
    if stopped is not None:
        value['activeTask'] = stopped.get('task', {'unavailable': True, 'error': 'cohort-bounded-cleanup-no-task-snapshot'})
    return {**value, 'stopRequested': True, 'cleanup': stopped}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest='action', required=True)
    commands.add_parser('describe', help='Read the strict cohort runtime fields and lifecycle contract')
    create = commands.add_parser('prepare', help='Offline freeze: existing comparison prepare for every AA experiment')
    for key in ('runtime', 'plan', 'out'):
        create.add_argument('--' + key, type=Path, required=True)
    for action in ('run', 'status', 'stop'):
        command = commands.add_parser(action)
        command.add_argument('--attempt', type=Path, required=True)
    args = cli.parse_args()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, cancel)
    try:
        if args.action == 'describe':
            value = {'schemaVersion': 1, 'kind': 'http-calibration-workflow-capability',
                     'runtimeKind': 'http-calibration-runtime',
                     'requiredFields': ['schemaVersion', 'kind', 'comparisonRuntime', 'cleanWindow', 'budgets'],
                     'comparisonRuntime': 'existing strict http-comparison-runtime/v1 (Node) or v2 (Go); no overrides',
                     'cleanWindow': {'systemctlExecutable': 'absolute path', 'requireClean': True,
                                     'units': '1..32 unique explicit systemd user .service unit names'},
                     'budgets': {'prepareMilliseconds': [1000, 7 * 86400000],
                                 'maxRunMilliseconds': [1000, 7 * 86400000], 'pollMilliseconds': [1000, 60000],
                                 'commandMilliseconds': [1000, 300000], 'maxCommandBytes': [65536, 16 * MiB],
                                 'maxWorkBytes': [MiB, 1024 ** 4], 'minFreeBytes': [0, 1024 ** 5],
                                 'cleanupMilliseconds': [5000, 20000]},
                     'execution': 'explicit generic task run; once only; sequential fixed child task identities',
                     'recovery': 'status/stop original cohort and task; no automatic resume/re-execution',
                     'qualificationAccepted': False, 'performanceAccepted': False}
        elif args.action == 'prepare':
            value = freeze(args.runtime, args.plan, args.out)
        elif args.action == 'run':
            value = Controller(args.attempt).run()
        else:
            value = {'status': status, 'stop': stop}[args.action](args.attempt)
        print(json.dumps({'ok': not value.get('failure'), 'data': value}, allow_nan=False))
        return 6 if value.get('failure') else 0
    except Exception as error:
        print(json.dumps({'ok': False, 'error': {'code': str(error) if isinstance(error, COMMON.WorkflowError)
              else type(error).__name__, 'retryable': False}}))
        return 6


if __name__ == '__main__':
    raise SystemExit(main())

