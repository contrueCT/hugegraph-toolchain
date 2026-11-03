#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to You under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Finite capacity exploration/confirmation through the existing comparison/task lifecycle.

No HTTP implementation, automatic restart, capacity-boundary classification or
statistical algorithm. A prepared attempt permits one dispatch. Inspect or stop
the same identities after a lost response; never resume the attempt.
"""
import argparse
import importlib.util
import json
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

sys.dont_write_bytecode = True
_SPEC = importlib.util.spec_from_file_location('hgbench_capacity_cohort_helpers',
                                               Path(__file__).with_name('calibration-workflow.py'))
COHORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(COHORT)
COMMON = COHORT.COMMON
need, read_json, entry = COMMON.need, COMMON.read_json, COMMON.file_entry
MiB = 1024 ** 2
CANCELLED = False
CANCELLED_AT = None
CONTROLS = ('capacity-workflow.py', 'calibration-workflow.py', 'comparison-workflow.py',
            'comparison-runtime.json', 'execution-runtime.json')


def control_files(runtime):
    return CONTROLS + COMMON.engine_control_files(runtime['comparisonRuntime'])


EXPLORATION_KEYS = ('id', 'index', 'caseId', 'mode', 'ratePerSecond', 'comparisonDirectory',
                    'comparisonPlanDigest', 'comparisonContentDigest')
SINGLE_EXPLORATION_KEYS = (*EXPLORATION_KEYS, 'expectedArms')
CONFIRMATION_KEYS = ('id', 'index', 'caseId', 'rateIndex', 'ratePerSecond', 'comparison', 'modes',
                     'expectedArms', 'sourcePoints', 'comparisonDirectory', 'comparisonPlanDigest',
                     'comparisonContentDigest')


def save(path, value, replace=False):
    COMMON.save(path, value, replace)
    descriptor = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def cancelled(_signal, _frame):
    global CANCELLED, CANCELLED_AT
    CANCELLED = True
    if CANCELLED_AT is None:
        CANCELLED_AT = time.monotonic()


def cancellation_deadline():
    # The existing generic task's parent TERM window is 30s. Keep 1s for final receipts/exit.
    return None if CANCELLED_AT is None else CANCELLED_AT + 29


def validate_runtime(value):
    COMMON.exact(value, ('schemaVersion', 'kind', 'cleanWindow', 'budgets'), 'capacity-runtime-fields')
    need(value['schemaVersion'] == 1 and value['kind'] == 'http-capacity-execution-runtime',
         'capacity-runtime-kind')
    clean = value['cleanWindow']
    COMMON.exact(clean, ('systemctlExecutable', 'requireClean', 'units'), 'capacity-clean-window-fields')
    COMMON.path_value(clean['systemctlExecutable'])
    need(clean['requireClean'] is True, 'capacity-clean-window-required')
    units = clean['units']
    need(isinstance(units, list) and 1 <= len(units) <= 32 and len(set(units)) == len(units) and
         all(isinstance(unit, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}\.service', unit)
             for unit in units), 'capacity-clean-window-units')
    budgets = value['budgets']
    COMMON.exact(budgets, ('prepareMilliseconds', 'commandMilliseconds', 'maxCommandBytes',
                          'maxWorkBytes', 'minFreeBytes', 'cleanupMilliseconds'), 'capacity-budget-fields')
    for key, low, high in [('prepareMilliseconds', 1000, 300000), ('commandMilliseconds', 1000, 300000),
                           ('maxCommandBytes', 65536, 16 * MiB), ('maxWorkBytes', MiB, 128 * 1024 ** 3),
                           ('minFreeBytes', 0, 1024 ** 5)]:
        COMMON.integer(budgets[key], low, high, 'capacity-' + key)
    need(budgets['cleanupMilliseconds'] == 20000, 'capacity-cleanup-must-fit-generic-task-grace')
    return value


def confirmation_command(argv, seconds, cap, env, guard, record=None):
    """Only fixed source verification needs TERM handlers to reap its detached replay workers."""
    guard()
    begun = COMMON.ns()
    process = subprocess.Popen(list(map(str, argv)), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               env=env, start_new_session=True)
    selected = selectors.DefaultSelector()
    output = {'stdout': bytearray(), 'stderr': bytearray()}
    error = None
    deadline = time.monotonic() + seconds
    def drain(wait):
        for key, _ in selected.select(max(0, wait)):
            raw = os.read(key.fd, 65536)
            if not raw:
                selected.unregister(key.fileobj)
            else:
                left = max(0, cap - sum(len(part) for part in output.values()))
                output[key.data].extend(raw[:left])
                need(len(raw) <= left, 'command-output-budget')
    try:
        for name in output:
            stream = getattr(process, name)
            os.set_blocking(stream.fileno(), False)
            selected.register(stream, selectors.EVENT_READ, name)
        while selected.get_map() or process.poll() is None:
            guard()
            need(time.monotonic() < deadline, 'command-deadline-exceeded')
            drain(min(0.05, max(0, deadline - time.monotonic())))
        process.wait(timeout=1)
    except BaseException as failure:
        error = str(failure) if isinstance(failure, COMMON.WorkflowError) else type(failure).__name__
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        closing = time.monotonic() + 10
        if cancellation_deadline() is not None:
            closing = min(closing, cancellation_deadline())
        # No repeated TERM and no reset grace for an individual descendant. The trusted CLI's
        # report handlers own their nested detached groups and must finish inside this one window.
        while time.monotonic() < closing and (selected.get_map() or process.poll() is None):
            try:
                drain(min(0.05, max(0, closing - time.monotonic())))
            except BaseException:
                # Output stays capped even while cancellation receipts are drained.
                pass
    finally:
        COMMON.kill_group(process)
        selected.close()
        process.stdout.close()
        process.stderr.close()
    value = {'argv': list(map(str, argv)), 'exitCode': process.returncode, 'error': error,
             'startedMonotonicNs': begun, 'finishedMonotonicNs': COMMON.ns(),
             **{name: raw.decode('utf-8', errors='replace') for name, raw in output.items()}}
    if record:
        save(record, value)
    need(error is None, error)
    return value


def invoke(runtime, args, seconds, check=lambda: None, record=None, failed_reply=False, graceful=False):
    command = confirmation_command if graceful else COMMON.command
    argv = COHORT.cli_argv(runtime, *args)
    env = COMMON.clean_env(runtime['comparisonRuntime']['hostPython'], runtime['comparisonRuntime']['nodeHome'])
    if graceful:
        raw = command(argv, seconds, runtime['budgets']['maxCommandBytes'], env, check, record)
    else:
        raw = command(argv, seconds, runtime['budgets']['maxCommandBytes'], env, check, record, False)
    if failed_reply:
        value = json.loads(raw['stdout'], object_pairs_hook=COMMON.unique)
        need(isinstance(value, dict) and value.get('ok') is True and isinstance(value.get('data'), dict),
             'capacity-task-response-unknown')
        return value['data'], raw['exitCode']
    return COMMON.response(raw)


def study(directory):
    """Only the two fixed capacity contracts; no user-provided dispatch recipe."""
    root = Path(directory)
    found = [phase for phase, filename in (('exploration', 'capacity-plan.json'),
                                           ('confirmation', 'confirmation-plan.json'))
             if (root / filename).exists()]
    need(len(found) == 1, 'capacity-one-plan-kind-required')
    phase = found[0]
    filename, lock = ('capacity-plan.json', 'capacity.lock.json') if phase == 'exploration' else (
        'confirmation-plan.json', 'confirmation.lock.json')
    plan = read_json(root / filename, 16 * MiB)
    need(plan.get('phase') == phase and plan.get('kind') == (
        'http-capacity-plan' if phase == 'exploration' else 'http-capacity-confirmation-plan'),
        'capacity-plan-kind-or-phase')
    if phase == 'exploration':
        budgets = {key: plan['policy'][key + 'Budgets'] for key in ('point', 'controller', 'study')}
        verify = ['http', 'capacity', 'verify']
        keys, review = EXPLORATION_KEYS, 0
        policy_version = plan['policy'].get('schemaVersion', 1)
        need(policy_version in (1, 2), 'capacity-exploration-policy-version')
        exploration = plan['policy'].get('exploration', {})
        if policy_version == 2:
            need(exploration.get('mode') == 'single-run' and type(exploration.get('runs')) is int and
                 exploration.get('runs') == 1 and
                 'pairs' not in exploration, 'capacity-single-observation-policy')
            keys = SINGLE_EXPLORATION_KEYS
        else:
            need(exploration.get('mode') != 'single-run' and 'runs' not in exploration,
                 'capacity-single-observation-requires-policy-two')
    else:
        budgets = {key: plan['budgets'][key] for key in ('point', 'controller', 'study')}
        verify = ['http', 'capacity', 'confirmation', 'verify']
        keys = CONFIRMATION_KEYS
        review = plan['estimate']['sourceReviewMaximumMilliseconds']
        COMMON.integer(review, 1000, 86400000, 'capacity-confirmation-source-review-budget')
        selections = plan['selection']['selections']
        need(isinstance(selections, list) and len(selections) == 11 * len(plan['modes']) and
             len({(s['caseId'], s['mode']) for s in selections}) == len(selections),
             'capacity-confirmation-selection-denominator')
    mode = 'stage-sum'
    run_ms = plan['estimate']['safetyUpperBoundMilliseconds']
    cleanup_ms = 100000
    explicit_budget = (phase == 'exploration' and policy_version == 2) or (
        phase == 'confirmation' and plan.get('schemaVersion', 1) == 2)
    if explicit_budget:
        mode = plan.get('budgetMode')
        need(mode in ('total', 'stage-sum') and plan.get('budgetMode') == mode and
             plan.get('cleanupReserveMilliseconds') == cleanup_ms,
             'capacity-budget-mode-or-reserve')
        if phase == 'exploration':
            need(mode == plan['policy'].get('budgetMode', 'total'), 'capacity-budget-mode-or-reserve')
        minimum = plan.get('minimumBudgetMilliseconds')
        COMMON.integer(minimum, cleanup_ms, 7 * 86400000, 'capacity-minimum-total-budget')
        declared_ms = budgets['study']['maxTotalMilliseconds']
        need(plan.get('totalDeadlineMilliseconds') == declared_ms and declared_ms >= minimum and
             (mode == 'total' or declared_ms >= run_ms),
             'capacity-declared-total-budget')
        if mode == 'total':
            run_ms = declared_ms
    return {'phase': phase, 'plan': plan, 'filename': filename, 'lock': lock,
            'verify': verify, 'keys': keys, 'budgets': budgets, 'reviewMs': review,
            'singleObservation': keys == SINGLE_EXPLORATION_KEYS, 'budgetMode': mode,
            'runMilliseconds': run_ms, 'cleanupReserveMilliseconds': cleanup_ms,
            'explicitBudgetMode': explicit_budget}


def confirmation_review_budget(info, requested):
    if info['phase'] != 'confirmation':
        return
    # One original-report replay during prepare, then independent pre/post replays during run.
    # The 10s local verification allowance is explicit; it is not an estimated normal duration.
    need(requested['budgets']['prepareMilliseconds'] >= info['reviewMs'] + 10000,
         'capacity-confirmation-prepare-review-budget')
    need(info['budgets']['controller']['totalMilliseconds'] >= 2 * (info['reviewMs'] + 10000) + 100000,
         'capacity-confirmation-controller-review-budget')


def confirmation_inputs_separate(info, output):
    if info['phase'] != 'confirmation':
        return
    reference = info['plan']['exploration']
    execution_set = Path(reference['executionSet']).resolve(strict=True)
    raw = COMMON.read_bytes(execution_set, MiB)
    need(COMMON.sha(raw) == reference['executionSetSha256'], 'capacity-confirmation-source-set-changed')
    supplied = json.loads(raw, object_pairs_hook=COMMON.unique)
    paths = [Path(reference[key]).resolve() for key in ('planDirectory', 'reportDirectory', 'executionSet')]
    paths.extend((execution_set.parent / point['executionDirectory']).resolve() for point in supplied['points'])
    need(all(output != path and not output.is_relative_to(path) and not path.is_relative_to(output)
             for path in paths), 'capacity-confirmation-output-in-source-evidence')


def freeze(runtime_file, capacity, output):
    started = time.monotonic()
    requested = validate_runtime(read_json(runtime_file, 65536))
    need(os.geteuid() != 0, 'capacity-nonroot-required')
    source_plan = Path(capacity).resolve(strict=True)
    info = study(source_plan)
    need(len(info['plan']['points']) > 0, 'capacity-confirmation-no-eligible-children')
    confirmation_review_budget(info, requested)
    shared_raw = COMMON.read_bytes(source_plan / 'project/runtime.json', 65536)
    shared = json.loads(shared_raw, object_pairs_hook=COMMON.unique)
    COMMON.validate_runtime(shared)
    source = Path(shared['sourceRoot']).resolve(strict=True)
    need(Path(__file__).resolve() == source / 'components/http/capacity-workflow.py',
         'capacity-current-source-root-required')
    for key in ('sourceRoot', 'nodeHome', 'hostPython', 'dockerExecutable'):
        need(str(Path(shared[key]).resolve(strict=True)) == shared[key], 'capacity-runtime-real-path-required')
    runtime = {**requested, 'comparisonRuntime': shared}
    root = Path(output).parent.resolve(strict=True) / Path(output).name
    need(not root.exists() and source_plan != root and not root.is_relative_to(source_plan) and
         not source_plan.is_relative_to(root), 'capacity-new-nonoverlapping-attempt-required')
    need(not root.is_relative_to(source), 'capacity-attempt-in-source')
    def check():
        need(not CANCELLED and time.monotonic() - started < requested['budgets']['prepareMilliseconds'] / 1000,
             'capacity-prepare-cancelled-or-deadline')
    verified = invoke(runtime, [*info['verify'], source_plan],
                      requested['budgets']['prepareMilliseconds'] / 1000, check,
                      graceful=info['phase'] == 'confirmation')
    plan = verified['plan']
    need(verified['valid'] is True and plan == info['plan'] and
         verified['planDigest'] == plan['planDigest'], 'capacity-plan-identity')
    safety = info['runMilliseconds']
    need(30000 <= safety <= 7 * 86400000 and
         info['budgets']['controller']['totalMilliseconds'] >= 100000,
         'capacity-controller-global-cleanup-budget')
    need(requested['budgets']['maxWorkBytes'] <= info['budgets']['study']['maxEvidenceBytes'] and
         requested['budgets']['minFreeBytes'] >= shared['supervisor']['minFreeBytes'],
         'capacity-work-or-free-reserve')
    need(Path(output).parent.resolve(strict=True) / Path(output).name == root,
         'capacity-output-parent-changed')
    confirmation_inputs_separate(info, root)
    root.mkdir(mode=0o700)
    usage = COMMON.ActiveUsage(root, requested['budgets']['maxWorkBytes'], 'capacity-prepare-work-budget',
                               requested['budgets']['minFreeBytes'])
    COMMON.copy_regular_tree(source_plan, root / 'capacity', info['budgets']['study']['maxPlanBytes'], check)
    (root / 'children').mkdir()
    (root / 'observations').mkdir()
    (root / 'prepare-commands').mkdir()
    with (root / 'comparison-runtime.json').open('xb') as stream:
        stream.write(shared_raw)
        stream.flush()
        os.fsync(stream.fileno())
    save(root / 'execution-runtime.json', requested)
    for name in (*CONTROLS[:3], *COMMON.engine_control_files(shared)):
        shutil.copyfile(Path(__file__).with_name(name), root / name)
    attempt_id = uuid.uuid4().hex
    points = []
    for point in plan['points']:
        request = 'capacity:' + attempt_id + ':' + point['id']
        points.append({key: point[key] for key in info['keys']})
        points[-1].update(directory=str(root / 'children' / point['id']), requestId=request,
                          taskId=COHORT.task_id(request))
    usage.sample()
    # Bind actual current implementation, Node and Python. No frozen target/JDK claims are minted here.
    identities = {'source': COMMON.inventory(source, COMMON.source_selection(shared), check),
                  'node': entry(Path(shared['nodeHome']) / 'bin/node', check=check),
                  'python': entry(shared['hostPython'], check=check)}
    if info['phase'] == 'exploration' and plan['policy'].get('continuation'):
        identities['docker'] = entry(shared['dockerExecutable'], check=check)
    workflow = {'schemaVersion': 1, 'kind': 'http-capacity-workflow-plan', 'attemptId': attempt_id,
                'root': str(root), 'owner': {'uid': os.geteuid(), 'gid': os.getegid()},
                'runtime': runtime, 'capacityPlanDigest': verified['planDigest'],
                'capacityContentDigest': verified['contentDigest'], 'points': points,
                'taskRoot': str(root / 'task-state'), 'requiredJobs': requested['cleanWindow']['units'],
                'controlFiles': [entry(root / name, name) for name in control_files(runtime)],
                'identities': identities, 'maxRunMilliseconds': safety,
                'budgetSemantics': 'existing comparison/task deadlines; 20s bounded cancellation cleanup; sampled work bytes',
                'replayAllowed': False, 'engineeringAccepted': False, 'performanceAccepted': False}
    save(root / 'workflow.json', workflow)
    digest = entry(root / 'workflow.json')['sha256']
    save(root / 'workflow.lock.json', {'schemaVersion': 1, 'kind': 'http-capacity-workflow-lock',
                                     'attemptId': attempt_id, 'planSha256': digest})
    return {'attempt': str(root), 'planSha256': digest, 'maxRunMilliseconds': safety,
            'hostPython': shared['hostPython'], 'points': len(points), 'executed': False,
            'phase': info['phase'], 'planFiles': [info['filename'], info['lock']],
            'replayAllowed': False, 'performanceAccepted': False}


def load(attempt, verify=False):
    root = Path(attempt).resolve(strict=True)
    need(not Path(attempt).is_symlink(), 'capacity-attempt-symlink')
    plan, lock = read_json(root / 'workflow.json'), read_json(root / 'workflow.lock.json')
    COMMON.exact(plan, ('schemaVersion', 'kind', 'attemptId', 'root', 'owner', 'runtime',
        'capacityPlanDigest', 'capacityContentDigest', 'points', 'taskRoot', 'requiredJobs',
        'controlFiles', 'identities', 'maxRunMilliseconds', 'budgetSemantics', 'replayAllowed',
        'engineeringAccepted', 'performanceAccepted'), 'capacity-workflow-fields')
    COMMON.exact(lock, ('schemaVersion', 'kind', 'attemptId', 'planSha256'), 'capacity-workflow-lock-fields')
    need(plan.get('kind') == 'http-capacity-workflow-plan' and plan.get('schemaVersion') == 1 and
         lock.get('kind') == 'http-capacity-workflow-lock' and lock.get('schemaVersion') == 1 and
         plan.get('root') == str(root) and lock.get('attemptId') == plan.get('attemptId') and
         isinstance(plan.get('attemptId'), str) and re.fullmatch('[a-f0-9]{32}', plan['attemptId']) and
         lock.get('planSha256') == entry(root / 'workflow.json')['sha256'] and
         plan.get('replayAllowed') is False and plan.get('performanceAccepted') is False,
         'capacity-workflow-identity')
    need(plan.get('owner') == {'uid': os.geteuid(), 'gid': os.getegid()} and os.geteuid() != 0,
         'capacity-owner-mismatch')
    runtime = plan['runtime']
    validate_runtime({k: runtime[k] for k in ('schemaVersion', 'kind', 'cleanWindow', 'budgets')})
    COMMON.validate_runtime(runtime['comparisonRuntime'])
    need({k: runtime[k] for k in ('schemaVersion', 'kind', 'cleanWindow', 'budgets')} ==
         read_json(root / 'execution-runtime.json') and runtime['comparisonRuntime'] ==
         read_json(root / 'comparison-runtime.json') and
         COMMON.read_bytes(root / 'comparison-runtime.json', 65536) ==
         COMMON.read_bytes(root / 'capacity/project/runtime.json', 65536), 'capacity-saved-runtime-drift')
    info = study(root / 'capacity')
    capacity = info['plan']
    confirmation_review_budget(info, runtime)
    need(plan['capacityPlanDigest'] == capacity['planDigest'] and
         plan['maxRunMilliseconds'] == info['runMilliseconds'] and
         100000 <= plan['maxRunMilliseconds'] <= 7 * 86400000,
         'capacity-frozen-budget-or-plan-drift')
    need(plan['taskRoot'] == str(root / 'task-state') and
         plan['requiredJobs'] == runtime['cleanWindow']['units'], 'capacity-path-or-job-drift')
    need([row['path'] for row in plan['controlFiles']] == list(control_files(runtime)), 'capacity-control-inventory')
    minimum, maximum = (11, 352) if info['phase'] == 'exploration' else (1, 66)
    need(isinstance(plan['points'], list) and minimum <= len(plan['points']) <= maximum, 'capacity-point-count')
    seen = set()
    for index, point in enumerate(plan['points']):
        COMMON.exact(point, (*info['keys'], 'directory', 'requestId', 'taskId'), 'capacity-point-fields')
        prefix = 'point' if info['phase'] == 'exploration' else 'confirm'
        need(point['id'] not in seen and re.fullmatch(prefix + r'-[a-f0-9]{32}', point['id']) and
             point['index'] == index and point['directory'] == str(root / 'children' / point['id']) and
             point['comparisonDirectory'] == 'points/' + point['id'] + '/comparison' and
             point['requestId'] == 'capacity:' + plan['attemptId'] + ':' + point['id'] and
             point['taskId'] == COHORT.task_id(point['requestId']), 'capacity-point-identity')
        seen.add(point['id'])
        if info['singleObservation']:
            need(type(point['expectedArms']) is int and point['expectedArms'] == 1,
                 'capacity-single-observation-point-shape')
        if info['phase'] == 'confirmation':
            need(point['expectedArms'] == 8 and point['comparison'] in ('aa', 'forward-embedded') and
                 set(point['modes']) == {'A', 'B'} and
                 all(mode in ('forward', 'embedded') for mode in point['modes'].values()) and
                 len(point['sourcePoints']) == (1 if point['comparison'] == 'aa' else 2),
                 'capacity-confirmation-point-shape')
    if verify:
        for row in plan['controlFiles']:
            need(entry(root / row['path'], row['path']) == row, 'capacity-control-changed')
        need(entry(__file__)['sha256'] == plan['controlFiles'][0]['sha256'], 'capacity-frozen-controller-required')
    return root, plan, lock['planSha256']


class Controller:
    # Reuse the existing strict seven-field systemd check and its explicit non-exclusive scope.
    clean_window = COHORT.Controller.clean_window

    def __init__(self, attempt):
        self.root, self.plan, self.sha = load(attempt)
        self.runtime = self.plan['runtime']
        self.started = time.monotonic()
        info = study(self.root / 'capacity')
        self.total_budget = info['budgetMode'] == 'total'
        self.hard_deadline = self.started + self.plan['maxRunMilliseconds'] / 1000
        self.active_deadline = self.hard_deadline - info['cleanupReserveMilliseconds'] / 1000
        self.cleanup = False
        self.cleanup_deadline = None
        self.step_deadline = None
        self.last_scan = 0
        self.active_usage = COMMON.ActiveUsage(self.root, self.runtime['budgets']['maxWorkBytes'],
                                               'capacity-work-budget', self.runtime['budgets']['minFreeBytes'])

    def stopping(self):
        global CANCELLED_AT
        marker = self.root / 'stop-request.json'
        if marker.exists():
            need(read_json(marker) == {'attemptId': self.plan['attemptId'], 'planSha256': self.sha},
                 'capacity-stop-identity')
            if CANCELLED_AT is None:
                CANCELLED_AT = time.monotonic()
            return True
        return CANCELLED

    def check(self):
        now = time.monotonic()
        if self.cleanup:
            need(self.cleanup_deadline is None or now < self.cleanup_deadline, 'capacity-cleanup-deadline')
            if self.total_budget:
                need(now < self.hard_deadline, 'capacity-study-cleanup-deadline')
            return
        need(not self.stopping(), 'capacity-stop-requested')
        # Keep the last 100s of the existing global allowance for failure cleanup/summary/task sealing.
        need(now < self.active_deadline if self.total_budget else
             now - self.started < (self.plan['maxRunMilliseconds'] - 100000) / 1000,
             'capacity-study-deadline')
        need(self.step_deadline is None or now < self.step_deadline, 'capacity-point-or-step-deadline')
        if now - self.last_scan >= 1:
            self.last_scan = now
            self.active_usage.sample()

    def source_current(self):
        shared = self.runtime['comparisonRuntime']
        expected = self.plan['identities']
        need(entry(Path(shared['nodeHome']) / 'bin/node', check=self.check) == expected['node'] and
             entry(shared['hostPython'], check=self.check) == expected['python'], 'capacity-runtime-changed')
        need(COMMON.inventory(shared['sourceRoot'], COMMON.source_selection(shared),
                              self.check) == expected['source'], 'capacity-source-changed')
        if 'docker' in expected:
            need(entry(shared['dockerExecutable'], check=self.check) == expected['docker'], 'capacity-docker-changed')

    def point_schedule(self, point, child_plan=None):
        """Read only the bound comparison schedule; single observation never invents a second arm."""
        info = study(self.root / 'capacity')
        source = self.root / 'capacity' / point['comparisonDirectory']
        original = read_json(source / 'plan.json')
        locked = read_json(source / 'plan.lock.json')
        need(original.get('planDigest') == point['comparisonPlanDigest'] and
             locked.get('contentDigest') == point['comparisonContentDigest'] and
             (child_plan is None or child_plan == original), 'capacity-point-schedule-drift')
        blocks = original.get('blocks')
        need(isinstance(blocks, list) and all(isinstance(block.get('arms'), list) for block in blocks),
             'capacity-point-arm-schedule')
        arms = [arm for block in blocks for arm in block['arms']]
        count = 8 if info['phase'] == 'confirmation' else (1 if info['singleObservation'] else 4)
        need(type(original.get('budgets', {}).get('armCount')) is int and
             original['budgets']['armCount'] == count and len(arms) == count and
             all(isinstance(arm.get('runId'), str) and re.fullmatch(r'arm-[a-f0-9]{40}', arm['runId'])
                 for arm in arms) and len({arm['runId'] for arm in arms}) == count,
             'capacity-point-arm-schedule')
        if info['singleObservation']:
            need(original.get('schemaVersion') == 3 and original.get('comparison') == 'single-observation' and
                 set(original.get('variants', {})) == {'O'} and len(blocks) == 1 and
                 original.get('design') == 'single-observation' and
                 blocks[0].get('order') == 'O' and arms[0].get('arm') == 'O' and
                 arms[0].get('position') == 0 and arms[0].get('expectedMode') == point['mode'] and
                 original['variants']['O'].get('mode') == point['mode'] and
                 original['budgets'].get('budgetMode') == info['budgetMode'],
                 'capacity-single-observation-arm-schedule')
        return arms

    def boundary(self, point, child, policy):
        """One fixed JS verifier owns raw/parent/runtime validation; Python consumes its bounded projection."""
        original = self.step_deadline
        self.step_deadline = min(original or float('inf'), time.monotonic() + policy['maxVerificationMilliseconds'] / 1000)
        output = self.root / 'observations' / (point['id'] + '-boundary.json')
        try:
            self.source_current()
            runtime = self.runtime['comparisonRuntime']
            argv = [Path(runtime['nodeHome']) / 'bin/node', Path(runtime['sourceRoot']) /
                    'components/http/capacity-evidence.mjs', '__boundary', '--plan', self.root / 'capacity',
                    '--point', point['id'], '--execution', Path(child['directory']) / 'execution', '--out', output,
                    '--max-bytes', str(policy['maxVerificationBytes'])]
            value = confirmation_command(argv, max(0.001, self.step_deadline - time.monotonic()),
                        min(65536, self.runtime['budgets']['maxCommandBytes']),
                        COMMON.clean_env(runtime['hostPython'], runtime['nodeHome']), self.check,
                        self.root / 'observations' / (point['id'] + '-boundary-command.json'))
            need(value['exitCode'] == 0, 'capacity-boundary-verification-failed')
            emitted = json.loads(value['stdout'], object_pairs_hook=COMMON.unique)
            raw = COMMON.read_bytes(output, MiB)
            need(emitted.get('ok') is True and emitted.get('receiptSha256') == COMMON.sha(raw),
                 'capacity-boundary-receipt-changed')
            result = json.loads(raw, object_pairs_hook=COMMON.unique)
            need(result.get('kind') == 'http-capacity-boundary-projection' and result.get('schemaVersion') == 1 and
                 result.get('capacityPlanDigest') == self.plan['capacityPlanDigest'] and
                 result.get('capacityContentDigest') == self.plan['capacityContentDigest'] and
                 result.get('pointId') == point['id'] and result.get('comparisonPlanDigest') == point['comparisonPlanDigest'] and
                 result.get('executionLockSha256') == entry(Path(child['directory']) / 'execution/execution.lock.json')['sha256'] and
                 type(result.get('plannedArms')) is int and type(result.get('observedArms')) is int and
                 result.get('plannedArms') == result.get('observedArms') == len(self.point_schedule(point)) and
                 result.get('action') in ('continue', 'stop-case', 'stop-study') and result.get('performanceAccepted') is False,
                 'capacity-boundary-identity')
            return result
        finally:
            self.step_deadline = original

    def docker_command(self, args, deadline, record):
        self.source_current()
        shared = self.runtime['comparisonRuntime']
        need('docker' in self.plan['identities'], 'capacity-docker-binding-missing')
        settings = read_json(self.root / 'capacity/project/settings.json')
        seconds = min(deadline - time.monotonic(), self.runtime['budgets']['commandMilliseconds'] / 1000)
        if self.step_deadline is not None:
            seconds = min(seconds, self.step_deadline - time.monotonic())
        need(seconds > 0, 'capacity-owned-check-deadline')
        return COMMON.command([shared['dockerExecutable'], '--host', 'unix://' + settings['dockerSocket'], *args],
                    seconds, min(65536, self.runtime['budgets']['maxCommandBytes']),
                    COMMON.clean_env(shared['hostPython'], shared['nodeHome']), self.check, record, False)

    def daemon(self, name, deadline):
        settings = read_json(self.root / 'capacity/project/settings.json')
        socket = Path(settings['dockerSocket']).resolve(strict=True)
        before = socket.stat()
        need(stat.S_ISSOCK(before.st_mode), 'capacity-docker-socket-kind')
        query = self.docker_command(['info', '--format', '{"ID":{{json .ID}},"ServerVersion":{{json .ServerVersion}}}'],
                    deadline, self.root / 'observations' / (name + '-daemon-command.json'))
        need(query['exitCode'] == 0 and query['error'] is None, 'capacity-docker-daemon-unavailable')
        value = json.loads(query['stdout'], object_pairs_hook=COMMON.unique)
        after = socket.stat()
        expected = read_json(self.root / 'capacity/project/environment.json')['docker']['serverVersion']
        COMMON.exact(value, ('ID', 'ServerVersion'), 'capacity-docker-daemon-fields')
        need(isinstance(value['ID'], str) and re.fullmatch(r'[A-Za-z0-9:-]{1,128}', value['ID']) and
             value['ServerVersion'] == expected and (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino),
             'capacity-docker-daemon-identity')
        return {**value, 'socketPath': str(socket), 'device': before.st_dev, 'inode': before.st_ino}

    def owned_absent(self, point, decision, baseline):
        deadline = time.monotonic() + 30
        original = self.step_deadline
        self.step_deadline = min(original or float('inf'), deadline)
        prefix = point['id'] + '-owned'
        try:
            owned = decision.get('ownedContainers')
            run_ids = {arm['runId'] for arm in self.point_schedule(point)}
            arm_count = len(run_ids)
            target_image = read_json(self.root / 'capacity/project/settings.json')['image']
            need(isinstance(owned, list) and len(owned) == 2 * arm_count and
                 len({row['id'] for row in owned}) == 2 * arm_count and
                 {row['runId'] for row in owned} == run_ids and all(
                    re.fullmatch('[a-f0-9]{64}', row['id']) and row['role'] in ('target', 'driver') and
                    row['imageId'] == (target_image if row['role'] == 'target' else
                                      self.runtime['comparisonRuntime']['driver']['image']) for row in owned) and
                 all({row['role'] for row in owned if row['runId'] == run} == {'target', 'driver'}
                     for run in {row['runId'] for row in owned}), 'capacity-owned-container-inventory')
            need(self.daemon(prefix + '-before', deadline) == baseline, 'capacity-docker-daemon-changed')
            for index, row in enumerate(owned):
                query = self.docker_command(['inspect', '--type', 'container', row['id']], deadline,
                            self.root / 'observations' / (prefix + '-' + str(index) + '.json'))
                need(query['exitCode'] == 1 and query['error'] is None and query['stdout'].strip() == '[]' and
                     re.fullmatch(r'(?:Error:|Error response from daemon:) No such container: ' +
                                  re.escape(row['id']) + r'\s*', query['stderr']), 'capacity-owned-absence-unproven')
            need(self.daemon(prefix + '-after', deadline) == baseline, 'capacity-docker-daemon-changed')
            save(self.root / 'observations' / (prefix + '-proof.json'), {
                'kind': 'http-capacity-owned-absence', 'pointId': point['id'], 'daemon': baseline,
                'containers': owned, 'cleanupProven': True, 'scope': 'exact recorded CIDs absent on this observed daemon/socket'})
        finally:
            self.step_deadline = original

    def cli(self, args, seconds=10, record=None, failed_reply=False, long=False):
        self.check()
        if self.total_budget:
            seconds = min(seconds, (self.hard_deadline if self.cleanup else self.active_deadline) - time.monotonic())
        if not long:
            seconds = min(seconds, self.runtime['budgets']['commandMilliseconds'] / 1000)
        if self.cleanup_deadline is not None:
            seconds = min(seconds, self.cleanup_deadline - time.monotonic())
        if self.step_deadline is not None and not self.cleanup:
            seconds = min(seconds, self.step_deadline - time.monotonic())
        need(seconds > 0, 'capacity-command-deadline')
        if args[:4] == ['http', 'capacity', 'confirmation', 'verify']:
            return invoke(self.runtime, args, seconds, self.check, record, failed_reply, graceful=True)
        return invoke(self.runtime, args, seconds, self.check, record, failed_reply)

    def clean(self, name):
        original = self.step_deadline
        self.step_deadline = min(original or float('inf'), time.monotonic() +
                                 min(10, self.runtime['budgets']['commandMilliseconds'] / 1000))
        try:
            return self.clean_window(name)
        finally:
            self.step_deadline = original

    def snapshot(self, child, seconds=5):
        value = self.cli(['--root', self.plan['taskRoot'], 'task', 'status', '--id', child['taskId']], seconds)
        need(value['taskId'] == child['taskId'] and value['planDigest'] == child['taskPlanDigest'] and
             type(value['terminal']) is bool and type(value['interrupted']) is bool,
             'capacity-task-identity')
        return value

    def stop_child(self, child):
        self.cleanup = True
        self.cleanup_deadline = time.monotonic() + self.runtime['budgets']['cleanupMilliseconds'] / 1000
        if self.total_budget:
            self.cleanup_deadline = min(self.cleanup_deadline, self.hard_deadline)
        if cancellation_deadline() is not None:
            self.cleanup_deadline = min(self.cleanup_deadline, cancellation_deadline())
        errors, terminal, compared = [], False, False
        # Never execute a changed public CLI to clean up. Independent child TTLs remain the containment boundary.
        try:
            self.source_current()
        except BaseException as error:
            return {'errors': [str(error)], 'taskTerminalProven': False,
                    'cleanupProven': False, 'reason': 'trusted-runtime-unavailable; inspect original task and TTL containment'}
        try:
            observed = self.snapshot(child, 2)
            save(self.root / 'observations' / (child['id'] + '-cleanup-initial-' + uuid.uuid4().hex + '.json'), observed)
        except BaseException as error:
            errors.append(str(error))
        for label, args in [
            ('task-cancel', ['--root', self.plan['taskRoot'], 'task', 'cancel', '--id', child['taskId']]),
            ('comparison-stop', ['http', 'comparison', 'stop', child['directory']]),
        ]:
            try:
                value = self.cli(args, 5, self.root / 'observations' / (child['id'] + '-' + label + '-' + uuid.uuid4().hex + '.json'))
                if label == 'comparison-stop':
                    compared = value.get('stopRequested') is True and not value.get('errors')
            except BaseException as error:
                errors.append(str(error))
        while time.monotonic() < self.cleanup_deadline:
            try:
                observed = self.snapshot(child, min(2, self.cleanup_deadline - time.monotonic()))
                if observed['terminal'] and not observed['interrupted'] and observed['state'] in ('completed', 'failed', 'cancelled'):
                    terminal = True
                    break
            except BaseException as error:
                errors.append(str(error))
            time.sleep(min(0.1, max(0, self.cleanup_deadline - time.monotonic())))
        return {'errors': errors[-10:], 'taskTerminalProven': terminal, 'stopCommandsAcknowledged': compared,
                'cleanupProven': False, 'scope': 'bounded task/stop coordination; owned container absence requires independent evidence'}

    def prepare_point(self, point):
        source_plan = self.root / 'capacity' / point['comparisonDirectory']
        prepared = self.cli(['http', 'comparison', 'prepare', self.root / 'comparison-runtime.json',
                             '--plan', source_plan, '--out', point['directory']], 330,
                            self.root / 'prepare-commands' / (point['id'] + '.json'), long=True)
        need(prepared['attempt'] == point['directory'] and prepared['executed'] is False and
             prepared['replayAllowed'] is False and prepared['taskPlan'] == str(Path(point['directory']) / 'task.json'),
             'capacity-point-preparation-identity')
        task = read_json(prepared['taskPlan'])
        need(task['digest'] == prepared['taskPlanDigest'], 'capacity-point-task-digest')
        child_plan = read_json(Path(point['directory']) / 'comparison/plan.json')
        child_lock = read_json(Path(point['directory']) / 'comparison/plan.lock.json')
        need(child_plan['planDigest'] == point['comparisonPlanDigest'] and
             child_lock['contentDigest'] == point['comparisonContentDigest'], 'capacity-prepared-comparison-drift')
        if study(self.root / 'capacity')['phase'] == 'confirmation':
            need(child_plan['budgets']['armCount'] == 8 and len(child_plan['blocks']) == 4 and
                 all(len(block['arms']) == 2 for block in child_plan['blocks']) and
                 child_plan['comparison'] == point['comparison'] and
                 {arm: child_plan['variants'][arm]['mode'] for arm in ('A', 'B')} == point['modes'],
                 'capacity-prepared-confirmation-arms-or-modes')
        if study(self.root / 'capacity')['singleObservation']:
            self.point_schedule(point, child_plan)
        frozen_runtime = read_json(Path(point['directory']) / 'runtime.json', 64 * MiB)
        identities = frozen_runtime['identities']
        node_binary = [row for row in identities['node'] if row.get('path') == 'bin/node' and
                       row.get('type') == 'file']
        expected = self.plan['identities']
        need(frozen_runtime['planDigest'] == point['comparisonPlanDigest'] and
             frozen_runtime['planContentDigest'] == point['comparisonContentDigest'] and
             frozen_runtime['settings'] == self.runtime['comparisonRuntime'] and
             identities['source'] == expected['source'] and identities['hostPython'] == expected['python'] and
             len(node_binary) == 1 and all(node_binary[0][key] == expected['node'][key]
                                          for key in ('bytes', 'sha256')),
             'capacity-prepared-runtime-drift')
        need(entry(Path(point['directory']) / 'runtime.lock.json')['sha256'] == prepared['runtimeDigest'],
             'capacity-prepared-runtime-lock-drift')
        return {**point, 'taskPlan': prepared['taskPlan'], 'taskPlanDigest': task['digest'],
                'taskPlanSha256': entry(prepared['taskPlan'])['sha256'],
                'runtimeDigest': prepared['runtimeDigest'],
                'taskTimeoutMs': task['plan']['timeoutMs']}

    def run(self):
        need(platform.system() == 'Linux' and os.geteuid() != 0, 'capacity-run-nonroot-linux-required')
        load(self.root, verify=True)
        # A new attempt may explicitly repeat the same scientific plan. This attempt can never resume.
        save(self.root / 'dispatch.json', {'attemptId': self.plan['attemptId'], 'planSha256': self.sha,
                                         'pid': os.getpid(), 'startedMonotonicNs': COMMON.ns()})
        info = study(self.root / 'capacity')
        row_keys = ('id', 'index', 'caseId', 'mode', 'ratePerSecond') if info['phase'] == 'exploration' else (
            'id', 'index', 'caseId', 'rateIndex', 'ratePerSecond', 'comparison', 'modes', 'expectedArms', 'sourcePoints')
        if info['singleObservation']:
            row_keys = (*row_keys, 'expectedArms')
        rows = [{**{key: p[key] for key in (*row_keys, 'requestId', 'taskId')},
                 'state': 'not-started', 'taskPlanDigest': None} for p in self.plan['points']]
        continuation = info['plan']['policy'].get('continuation') if info['phase'] == 'exploration' else None
        stopped_cases, daemon_baseline = {}, None
        failure, active, cleanup = None, None, None
        try:
            self.source_current()
            verify_seconds = 30 if info['phase'] == 'exploration' else info['reviewMs'] / 1000 + 10
            verified = self.cli([*info['verify'], self.root / 'capacity'], verify_seconds,
                                long=info['phase'] == 'confirmation')
            need(verified['planDigest'] == self.plan['capacityPlanDigest'] and
                 verified['contentDigest'] == self.plan['capacityContentDigest'], 'capacity-plan-changed')
            expected = [{**{k: p[k] for k in info['keys']}}
                        for p in verified['plan']['points']]
            need(expected == [{k: p[k] for k in expected[0]} for p in self.plan['points']],
                 'capacity-point-order-changed')
            if continuation:
                daemon_baseline = self.daemon('initial', time.monotonic() + 30)
                save(self.root / 'observations/docker-daemon-baseline.json', daemon_baseline)
            for point, row in zip(self.plan['points'], rows):
                self.check()
                if point['caseId'] in stopped_cases:
                    row.update(skipReason='verified-case-range-stop', triggerPointId=stopped_cases[point['caseId']],
                               dispatchRecorded=False)
                    continue
                self.step_deadline = time.monotonic() + (
                    info['budgets']['point']['maxTotalMilliseconds'] +
                    info['budgets']['controller']['perPointMilliseconds']) / 1000
                if self.total_budget:
                    self.step_deadline = min(self.step_deadline, self.active_deadline)
                self.source_current()
                self.clean(point['id'] + '-before')
                row['state'] = 'preparing'
                child = self.prepare_point(point)
                self.check()
                row.update(taskPlanDigest=child['taskPlanDigest'], state='prepared')
                save(self.root / 'observations' / (point['id'] + '-prepared.json'), child)
                self.source_current()
                # This active identity is durable before the sole public submission call.
                active = child
                save(self.root / 'current.json', {'planSha256': self.sha, 'child': child}, replace=True)
                save(self.root / 'observations' / (point['id'] + '-dispatch.json'),
                     {'planSha256': self.sha, 'child': child, 'atMonotonicNs': COMMON.ns()})
                row['state'] = 'dispatch-recorded'
                try:
                    observed, rc = self.cli(['--root', self.plan['taskRoot'], 'task', 'run', '--plan',
                        child['taskPlan'], '--request-id', child['requestId']], child['taskTimeoutMs'] / 1000 + 30,
                        self.root / 'observations' / (point['id'] + '-foreground.json'), True, long=True)
                    need(observed['taskId'] == child['taskId'] and observed['planDigest'] == child['taskPlanDigest'] and
                         observed.get('reused') is not True and observed.get('terminal') is True and
                         observed.get('interrupted') is False, 'capacity-task-terminal-identity')
                    row.update(state=observed['state'], terminal=True, interrupted=False)
                    save(self.root / 'observations' / (point['id'] + '-terminal.json'), observed)
                    need((rc == 0 and observed['state'] == 'completed') or
                         (continuation and rc == 6 and observed['state'] == 'failed'),
                         'capacity-point-failed-no-boundary-claim')
                except BaseException:
                    if row['state'] == 'dispatch-recorded':
                        row['state'] = 'submission-response-unknown'
                    raise
                result = read_json(Path(child['directory']) / 'result.json')
                need(result.get('kind') == 'http-comparison-workflow-result' and result.get('executionCompleted') is True and
                     ((not continuation and result.get('ok') is True and result.get('evidenceValid') is True) or
                      (continuation and result.get('failure') is None and result.get('bridgeError') is None and
                       result.get('bridgeExitCode') == 0 and type(result.get('evidenceValid')) is bool and
                       result.get('ok') is result.get('evidenceValid') and
                       result.get('ok') is (observed['state'] == 'completed'))),
                     'capacity-point-evidence-invalid-no-boundary-claim')
                terminal = read_json(Path(child['directory']) / 'execution/terminal.json')
                registration = read_json(Path(child['directory']) / 'execution/execution.json')
                execution_lock = read_json(Path(child['directory']) / 'execution/execution.lock.json')
                need(terminal.get('status') == 'completed' and terminal.get('runtimeReverified') is True and
                     terminal.get('failure') is None and terminal.get('planDigest') == point['comparisonPlanDigest'] and
                     registration.get('planDigest') == point['comparisonPlanDigest'] and
                     execution_lock.get('planDigest') == point['comparisonPlanDigest'] and
                     terminal.get('runtimeDigest') == registration.get('runtimeDigest') == child['runtimeDigest'],
                     'capacity-point-sealed-lifecycle-unproven')
                self.source_current()
                if continuation:
                    decision = self.boundary(point, child, continuation)
                    row['boundaryDecision'] = decision
                    need(decision['action'] != 'stop-study', 'capacity-unexplained-or-invalid-boundary')
                    self.owned_absent(point, decision, daemon_baseline)
                    row['ownedCleanupProven'] = True
                    if decision['action'] == 'stop-case':
                        stopped_cases[point['caseId']] = point['id']
                        row['caseStopTrigger'] = True
                self.clean(point['id'] + '-after')
                active = None
                self.step_deadline = None
            if info['phase'] == 'confirmation':
                # Original exploration/report provenance is rechecked after the last selected child.
                # This is outside all measured child workflows and uses its declared global allowance.
                self.source_current()
                after = self.cli([*info['verify'], self.root / 'capacity'], verify_seconds, long=True)
                need(after['planDigest'] == self.plan['capacityPlanDigest'] and
                     after['contentDigest'] == self.plan['capacityContentDigest'], 'capacity-plan-changed')
                self.source_current()
        except BaseException as error:
            failure = str(error) if isinstance(error, COMMON.WorkflowError) else type(error).__name__
        finally:
            self.cleanup = True
            self.step_deadline = None
            if active is not None:
                cleanup = self.stop_child(active)
            mappings = [{'id': p['id'], 'executionDirectory': 'children/' + p['id'] + '/execution'}
                        for p in self.plan['points'] if (Path(p['directory']) / 'execution').is_dir()
                        and not (Path(p['directory']) / 'execution').is_symlink()]
            save(self.root / 'execution-set.json', {'schemaVersion': 1, 'kind': 'http-capacity-execution-set',
                                                   'planDigest': self.plan['capacityPlanDigest'], 'points': mappings})
            result = {'schemaVersion': 1, 'kind': 'http-capacity-workflow-result',
                      'attemptId': self.plan['attemptId'], 'planSha256': self.sha,
                      'planDigest': self.plan['capacityPlanDigest'], 'ok': failure is None,
                      'failure': failure, 'cleanup': cleanup, 'points': rows,
                      'finishedMonotonicNs': COMMON.ns(), 'wholeWindowExclusivityProven': False,
                      'boundaryClassification': 'none; any failure stops the full study',
                      'engineeringAccepted': False, 'statisticsCertified': False, 'performanceAccepted': False}
            if continuation:
                result.update(continuation=continuation, stoppedCases=stopped_cases,
                    boundaryClassification='verified complete driver/SLO range symptoms; remaining same-case conditions unexecuted; target upper bound unknown')
            if info['phase'] == 'confirmation':
                result.update(phase='confirmation', selections=info['plan']['selection']['selections'],
                              selectionRows=len(info['plan']['selection']['selections']),
                              plannedComparisons=len(self.plan['points']), confirmationQualified=False)
            if info['singleObservation']:
                result.update(explorationMode='single-run', plannedArms=len(self.plan['points']))
            if info['explicitBudgetMode']:
                result.update(budgetMode=info['budgetMode'], totalDeadlineMilliseconds=self.plan['maxRunMilliseconds'],
                              cleanupReserveMilliseconds=info['cleanupReserveMilliseconds'])
            save(self.root / 'result.json', result)
            save(self.root / 'active-usage.json', self.active_usage.audit)
            audit_entries = []
            if continuation:
                names = ['initial-daemon-command.json', 'docker-daemon-baseline.json']
                for point in self.plan['points']:
                    owned_count = 2 * (1 if info['singleObservation'] else 4)
                    names.extend(point['id'] + suffix for suffix in (
                        '-boundary.json', '-boundary-command.json', '-owned-before-daemon-command.json',
                        '-owned-after-daemon-command.json', '-owned-proof.json',
                        *('-owned-' + str(index) + '.json' for index in range(owned_count))))
                for name in sorted(names):
                    path = self.root / 'observations' / name
                    if path.exists():
                        raw = COMMON.read_bytes(path, MiB)
                        audit_entries.append({'path': 'observations/' + name, 'bytes': len(raw), 'sha256': COMMON.sha(raw)})
            save(self.root / 'evidence.lock.json', {'schemaVersion': 1, 'kind': 'http-capacity-workflow-summary-lock',
                 'attemptId': self.plan['attemptId'], 'planSha256': self.sha,
                 'files': [entry(self.root / name, name) for name in
                           ('dispatch.json', 'execution-set.json', 'result.json', 'active-usage.json')] + audit_entries,
                 'scope': ('controller summaries and continuation raw observations; independently verify each child execution and all planned points'
                           if continuation else 'controller summaries only; independently verify each child execution and all planned points')})
        return result


def status(attempt, read_task=True):
    controller = Controller(attempt)
    value = {'attempt': str(controller.root), 'attemptId': controller.plan['attemptId'],
             'planSha256': controller.sha, 'state': 'planned', 'replayAllowed': False,
             'recovery': 'inspect/stop this attempt and original task; new execution needs explicit new attempt',
             'performanceAccepted': False}
    if (controller.root / 'dispatch.json').exists():
        value['state'] = 'dispatch-recorded'
    if (controller.root / 'result.json').exists():
        value.update(state='terminal', result=read_json(controller.root / 'result.json'))
    if read_task and (controller.root / 'current.json').exists():
        child = current(controller)
        controller.cleanup = True
        controller.cleanup_deadline = time.monotonic() + 5
        try:
            controller.source_current()
            value['activeTask'] = controller.snapshot(child, 2)
        except BaseException as error:
            value['activeTask'] = {'unavailable': True, 'error': str(error)}
    return value


def current(controller):
    value = read_json(controller.root / 'current.json')
    need(value['planSha256'] == controller.sha, 'capacity-current-plan-identity')
    child = value['child']
    point = next(p for p in controller.plan['points'] if p['id'] == child['id'])
    need(all(child[key] == item for key, item in point.items()) and
         child['taskPlan'] == str(Path(point['directory']) / 'task.json') and
         entry(child['taskPlan'])['sha256'] == child['taskPlanSha256'] and
         read_json(child['taskPlan'])['digest'] == child['taskPlanDigest'] and
         entry(Path(child['directory']) / 'runtime.lock.json')['sha256'] == child['runtimeDigest'],
         'capacity-current-child-identity')
    return child


def stop(attempt):
    controller = Controller(attempt)
    marker = controller.root / 'stop-request.json'
    wanted = {'attemptId': controller.plan['attemptId'], 'planSha256': controller.sha}
    if not marker.exists():
        try:
            COMMON.publish_marker(marker, wanted)
        except FileExistsError:
            pass
    need(read_json(marker) == wanted, 'capacity-stop-identity')
    cleanup = controller.stop_child(current(controller)) if (controller.root / 'current.json').exists() else None
    return {**status(attempt, read_task=False), 'stopRequested': True, 'cleanup': cleanup}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    commands.add_parser('describe')
    create = commands.add_parser('prepare')
    for key in ('runtime', 'plan', 'out'):
        create.add_argument('--' + key, type=Path, required=True)
    for action in ('run', 'status', 'stop'):
        commands.add_parser(action).add_argument('--attempt', type=Path, required=True)
    args = parser.parse_args()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, cancelled)
    try:
        if args.action == 'describe':
            value = {'kind': 'http-capacity-workflow-capability', 'runtimeKind': 'http-capacity-execution-runtime',
                     'phases': ['exploration', 'confirmation'], 'confirmationRequiresNonemptyPlan': True,
                     'dispatch': 'one generic task; sequential public comparison prepare and one foreground task run per point',
                     'cleanupMilliseconds': 20000, 'terminateGraceMilliseconds': 30000,
                     'retry': 'none; original identity status/stop only', 'performanceAccepted': False}
        elif args.action == 'prepare':
            value = freeze(args.runtime, args.plan, args.out)
        elif args.action == 'run':
            value = Controller(args.attempt).run()
        else:
            value = {'status': status, 'stop': stop}[args.action](args.attempt)
        print(json.dumps({'schemaVersion': 1, 'ok': not value.get('failure'), 'data': value}, allow_nan=False))
        return 6 if value.get('failure') else 0
    except BaseException as error:
        print(json.dumps({'schemaVersion': 1, 'ok': False, 'error': {
            'code': str(error) if isinstance(error, COMMON.WorkflowError) else type(error).__name__,
            'retryable': False}, 'performanceAccepted': False}))
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
