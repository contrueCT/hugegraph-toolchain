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

"""Freeze and supervise a complete SF100 reference import in an isolated Docker container.

plan hashes small metadata/runtime files, not full CSV data. run rechecks every CSV before import.
The container supervises its own child and disk/time budgets even if the submitting client disconnects.
No downloads, --force, automatic retry, database server, or performance acceptance.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
GIB = 1024 ** 3
STOP = False
STRICT = ['--id-type=INTEGER', '--delimiter=|', '--array-delimiter=;', '--input-encoding=UTF-8',
          '--legacy-style-quoting=false', '--trim-strings=false', '--ignore-extra-columns=false',
          '--ignore-empty-strings=false', '--skip-bad-relationships=false', '--skip-duplicate-nodes=false',
          '--bad-tolerance=0', '--multiline-fields=false', '--normalize-types=true',
          '--auto-skip-subsequent-headers=false', '--cache-on-heap=false', '--high-io=false']
NEO4J_CONFIG = ('dbms.default_database=neo4j\ndbms.directories.data=/work/data\n'
               'dbms.directories.logs=/work/logs\ndbms.directories.transaction.logs.root=/work/transactions\n'
               'dbms.directories.import=/input\n')


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def file_hash(path, guard=lambda: None):
    h = hashlib.sha256()
    with Path(path).open('rb') as source:
        for raw in iter(lambda: source.read(1024 * 1024), b''):
            guard()
            h.update(raw)
    return h.hexdigest()


def read_json(path, limit=8 * 1024**2):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('Expected bounded regular JSON: ' + str(path))
    raw = path.read_bytes()
    return json.loads(raw), sha(raw)


def publish(path, value):
    with Path(path).open('xb') as f:
        f.write(encoded(value))
        f.flush()
        os.fsync(f.fileno())


def relative_path(value):
    if not isinstance(value, str) or '\\' in value or '\x00' in value:
        raise ValueError('Invalid artifact path')
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or '..' in path.parts or str(path) != value:
        raise ValueError('Artifact must use a canonical contained relative path')
    return value


def regular(root, relative, size=None):
    path = Path(root) / relative_path(relative)
    if path.is_symlink() or not path.is_file() or path.resolve() != path.absolute():
        raise ValueError('Input must be a regular file without symlink components: ' + str(path))
    if size is not None and path.stat().st_size != size:
        raise ValueError('Input byte length changed: ' + str(path))
    return path


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('Invalid budget ' + name)
    return value


def validate_budget(value):
    fields = {'cpus', 'memoryBytes', 'heapMiB', 'offHeapMiB', 'maxSeconds', 'workspaceMaxBytes',
              'reserveFreeBytes', 'maxLogBytes', 'pidsMax'}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('Budget fields differ from the import contract')
    for name, low, high in [('cpus', 1, 64), ('memoryBytes', GIB, 128*GIB), ('heapMiB', 128, 65536),
                            ('offHeapMiB', 128, 65536), ('maxSeconds', 60, 172800),
                            ('workspaceMaxBytes', GIB, 16*1024*GIB), ('reserveFreeBytes', 0, 16*1024*GIB),
                            ('maxLogBytes', 65536, 64*1024**2), ('pidsMax', 64, 1024)]:
        integer(value[name], name, low, high)
    if (value['heapMiB'] + value['offHeapMiB']) * 1024**2 + GIB > value['memoryBytes']:
        raise ValueError('Container memory must include heap, import cache and at least 1 GiB native margin')
    return value


def bridge():
    spec = importlib.util.spec_from_file_location('reference_prepare', ROOT / 'tools/ldbc-reference-prepare.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_manifest(prepared, catalog_path, inventory_path, input_root=None):
    source = bridge()
    manifest, manifest_sha = read_json(prepared)
    catalog, catalog_sha = read_json(catalog_path)
    inventory, inventory_sha = read_json(inventory_path)
    asset, members = source.validated_inputs(catalog, inventory)
    specs, reference = source.schema()
    if (asset['scale'] != '100' or manifest.get('schemaVersion') != 1 or
            manifest.get('status') != 'converted-not-imported' or manifest.get('inputAsset') != asset or
            manifest.get('sourceVerification') != 'same-stream-compressed-and-all-member-sha256' or
            manifest.get('catalogSha256') != catalog_sha or manifest.get('inventorySha256') != inventory_sha or
            manifest.get('reference') != reference or catalog.get('reference', {}).get('commit') != reference['commit']):
        raise ValueError('Complete reference manifest does not match frozen SF100 inputs/mapping')
    counts = Counter()
    for name, entry in members.items():
        table = source.table_of(source.member_path(name))
        if table:
            if table not in specs and table not in source.MULTI:
                raise ValueError('Unmapped graph table')
            counts[table] += integer(entry.get('rows'), 'inventory rows', 0, 2**63-1)
    if (set(counts) != set(specs) | set(source.MULTI) or manifest.get('counts') != dict(counts) or
            any(type(x) is not int for x in manifest['counts'].values())):
        raise ValueError('Reference table counts differ from the entire archive')
    if manifest.get('memberCount') != len(members):
        raise ValueError('Reference archive member coverage differs')
    files = manifest.get('files')
    if not isinstance(files, list) or len(files) != len(specs):
        raise ValueError('Reference CSV inventory must cover every mapped table exactly once')
    by_path = {}
    for item in files:
        if set(item) != {'path', 'sha256', 'bytes', 'rows'}:
            raise ValueError('Invalid CSV artifact contract')
        relative_path(item['path'])
        if item['path'] in by_path or not re.fullmatch('[a-f0-9]{64}', item.get('sha256', '')):
            raise ValueError('Duplicate CSV path or invalid digest')
        integer(item['bytes'], 'CSV bytes', 1, 16*1024*GIB)
        integer(item['rows'], 'CSV rows', 0, 2**63-1)
        by_path[item['path']] = item
    root = Path(input_root) if input_root is not None else Path(prepared).parent
    tables = []
    for name, spec in sorted(specs.items()):
        relative = spec['category'] + '/' + name + '_0_0.csv'
        item = by_path.get(relative)
        if item is None or item['rows'] != counts[name]:
            raise ValueError('CSV coverage or row count mismatch: ' + name)
        path = regular(root, relative, item['bytes'])
        with path.open('rb') as f:
            if f.readline(1024*1024+1) != source.encode_row(spec['columns']):
                raise ValueError('Reference CSV header mismatch: ' + name)
        tables.append({**item, 'table': name, 'kind': spec['kind'], 'label': spec['label']})
    if manifest.get('csvBytes') != sum(item['bytes'] for item in files):
        raise ValueError('CSV total differs from manifest')
    return {'manifestSha256': manifest_sha, 'catalogSha256': catalog_sha, 'inventorySha256': inventory_sha,
            'reference': reference, 'tables': tables, 'csvBytes': manifest['csvBytes'],
            'expected': {kind: sum(t['rows'] for t in tables if t['kind'] == kind)
                         for kind in ('nodes', 'relationships')}}


def tracked(root, paths):
    return [{'path': str(p.relative_to(root)), 'bytes': p.stat().st_size, 'sha256': file_hash(p)} for p in paths]


def tree_links(root):
    root = Path(root)
    result = []
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            target = os.readlink(path)
            if Path(target).is_absolute() or not path.resolve(strict=True).is_relative_to(root.resolve()):
                raise ValueError('Runtime links must remain relative and contained')
            result.append({'path': str(path.relative_to(root)), 'target': target})
    return result


def verify_tree(root, files, links, guard=lambda: None):
    root = Path(root)
    names = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and not p.is_symlink()}
    if names != {item['path'] for item in files} or tree_links(root) != links:
        raise ValueError('Runtime file/link inventory changed')
    verify_files(root, files, guard)


def make_plan(args):
    facts = validate_manifest(args.prepared, args.catalog, args.inventory)
    budget = validate_budget(read_json(args.budget)[0])
    runtime_root = args.runtime_root.resolve(strict=True)
    runtime, runtime_sha = read_json(args.runtime_lock)
    if runtime.get('version') != '4.4.24' or runtime.get('edition') != 'community':
        raise ValueError('Expected pinned Neo4j Community 4.4.24 runtime')
    files = runtime.get('files')
    if not isinstance(files, list) or not 1 <= len(files) <= 4096:
        raise ValueError('Invalid runtime inventory')
    names = set()
    for f in files:
        if set(f) != {'path', 'sha256', 'bytes'} or f['path'] in names or not re.fullmatch('[a-f0-9]{64}', f['sha256']):
            raise ValueError('Invalid runtime file identity')
        names.add(f['path'])
        regular(runtime_root, f['path'], f['bytes'])
    if 'neo4j-community-4.4.24/bin/neo4j-admin' not in names:
        raise ValueError('Importer launcher missing from runtime inventory')
    dist = runtime_root/'neo4j-community-4.4.24'
    if {str(p.relative_to(runtime_root)) for p in dist.rglob('*') if p.is_file() and not p.is_symlink()} != names:
        raise ValueError('Distribution contains missing or untracked runtime files')
    java = args.java_home.resolve(strict=True)
    regular(java, 'bin/java')
    if not re.fullmatch('sha256:[a-f0-9]{64}', args.image):
        raise ValueError('Docker image must be a content-addressed ID')
    workspace = args.workspace.absolute()
    if (workspace.exists() or workspace.is_symlink() or not workspace.parent.is_dir() or
            workspace.parent.resolve() != workspace.parent):
        raise ValueError('Workspace must be a new path under an existing real parent')
    input_root = args.prepared.resolve().parent
    for root in [input_root, runtime_root, java, ROOT.resolve()]:
        if workspace == root or workspace in root.parents or root in workspace.parents:
            raise ValueError('Workspace must not overlap an input/runtime/source directory')
    component_paths = [ROOT/'tools'/name for name in
                       ['ldbc-reference-import.py', 'ldbc-reference-prepare.py', 'ldbc-archive.py']]
    component_paths += sorted(p for p in (ROOT/'adapters/ldbc-reference/upstream').rglob('*') if p.is_file())
    content = {'schemaVersion': 1, 'kind': 'sf100-reference-import-plan', 'facts': facts, 'budget': budget,
               'workspace': str(workspace), 'inputRoot': str(input_root),
               'metadata': {'manifest': str(args.prepared.resolve()), 'catalog': str(args.catalog.resolve()),
                            'inventory': str(args.inventory.resolve())},
               'runtimeRoot': str(runtime_root), 'runtimeLockSha256': runtime_sha, 'runtimeFiles': files,
               'runtimeLinks': tree_links(dist), 'javaLinks': tree_links(java),
               'javaHome': str(java), 'javaFiles': tracked(java, sorted(p for p in java.rglob('*')
                    if p.is_file() and not p.is_symlink())), 'imageId': args.image,
               'componentRoot': str(ROOT.resolve()), 'componentFiles': tracked(ROOT, component_paths),
               'databaseName': 'neo4j', 'strictImportArguments': STRICT, 'dataCorrectnessAccepted': False}
    return {**content, 'digest': sha(encoded(content))}


def load_plan(path):
    plan, _ = read_json(path)
    content = {k: v for k, v in plan.items() if k != 'digest'}
    if plan.get('digest') != sha(encoded(content)) or plan.get('kind') != 'sf100-reference-import-plan' or plan.get('schemaVersion') != 1:
        raise ValueError('Import plan identity changed')
    validate_budget(plan['budget'])
    if plan['strictImportArguments'] != STRICT or plan['databaseName'] != 'neo4j':
        raise ValueError('Strict import policy changed')
    return plan


def importer_arguments(plan):
    b = plan['budget']
    return ['/neo4j/bin/neo4j-admin', 'import', '--database=neo4j', *STRICT,
            '--max-memory=' + str(b['offHeapMiB']) + 'm', '--processors=' + str(b['cpus']),
            '--report-file=/work/import.report', *['--' + t['kind'] + '=' + t['label'] + '=/input/' + t['path']
                                                  for t in plan['facts']['tables']]]


def verify_files(root, files, guard=lambda: None):
    for item in files:
        path = regular(root, item['path'], item['bytes'])
        if file_hash(path, guard) != item['sha256']:
            raise ValueError('Frozen file changed: ' + str(path))


def input_fingerprints(root, files):
    result = []
    for item in files:
        stat = regular(root, item['path'], item['bytes']).stat()
        result.append({'path': item['path'], 'device': stat.st_dev, 'inode': stat.st_ino,
                       'size': stat.st_size, 'mtimeNs': stat.st_mtime_ns, 'ctimeNs': stat.st_ctime_ns})
    return result


def docker(*argv, timeout=30):
    return subprocess.check_output(['docker', *map(str, argv)], text=True, stderr=subprocess.STDOUT,
                                   timeout=timeout).strip()


def dispatch(plan_file):
    plan = load_plan(plan_file)
    if os.getuid() == 0:
        raise ValueError('Import container must use a non-root submitting user')
    # Recompute semantic metadata before any output or container creation.
    facts = validate_manifest(plan['metadata']['manifest'], plan['metadata']['catalog'], plan['metadata']['inventory'])
    if facts != plan['facts']:
        raise ValueError('Reference facts changed after planning')
    verify_files(plan['componentRoot'], plan['componentFiles'])
    image = json.loads(docker('image', 'inspect', plan['imageId']))[0]
    if image['Id'] != plan['imageId']:
        raise ValueError('Docker image identity differs')
    workspace = Path(plan['workspace'])
    if shutil.disk_usage(workspace.parent).free < plan['budget']['reserveFreeBytes'] + plan['budget']['workspaceMaxBytes']:
        raise ValueError('Complete import workspace and reserve do not fit')
    workspace.mkdir(mode=0o700)  # Never reuse an attempt, including a failed preflight.
    output = workspace/'output'
    output.mkdir()
    control = workspace / 'control'
    control.mkdir()
    for item in plan['componentFiles']:
        target = control/'component'/item['path']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Path(plan['componentRoot'])/item['path'], target)
    for key, source in plan['metadata'].items():
        shutil.copyfile(source, control/(key+'.json'))
    publish(control/'plan.json', plan)
    conf = control/'conf'
    conf.mkdir()
    (conf/'neo4j.conf').write_text(NEO4J_CONFIG)
    b = plan['budget']
    name = 'hgbench-reference-' + plan['digest'][:20]
    argv = ['create', '--name', name, '--label', 'hgbench.owner=sf100-reference-import',
            '--label', 'hgbench.plan='+plan['digest'], '--label', 'hgbench.workspace='+str(workspace),
            '--network', 'none', '--user', str(os.getuid())+':'+str(os.getgid()), '--init', '--read-only',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--restart', 'no',
            '--memory', str(b['memoryBytes']), '--memory-swap', str(b['memoryBytes']),
            '--cpus', str(b['cpus']), '--pids-limit', str(b['pidsMax']),
            '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=268435456',
            '--log-driver', 'local', '--log-opt', 'max-size=4m', '--log-opt', 'max-file=3',
            '--env', 'JAVA_HOME=/jdk', '--env', 'NEO4J_HOME=/neo4j', '--env', 'NEO4J_CONF=/control/conf',
            '--env', 'HEAP_SIZE='+str(b['heapMiB'])+'m', '--env', 'PYTHONDONTWRITEBYTECODE=1']
    argv += ['--env', 'HGBENCH_REFERENCE_CONTAINER='+plan['digest']]
    mounts = [(output, '/work', False), (control, '/control', True),
              (Path(plan['inputRoot']), '/input', True),
              (Path(plan['runtimeRoot'])/'neo4j-community-4.4.24', '/neo4j', True),
              (Path(plan['javaHome']), '/jdk', True)]
    for source, dest, readonly in mounts:
        if ',' in str(source):
            raise ValueError('Docker mount paths cannot contain commas')
        argv += ['--mount', 'type=bind,src='+str(source)+',dst='+dest+(',readonly' if readonly else '')]
    argv += [plan['imageId'], '/usr/local/bin/python3',
             '/control/component/tools/ldbc-reference-import.py', 'container-worker', '--plan', '/control/plan.json']
    publish(workspace/'dispatch-intent.json', {'atUnix': time.time(), 'argv': ['docker', *argv],
                                             'planDigest': plan['digest']})
    cid = docker(*argv)
    publish(workspace/'container.json', {'id': cid, 'name': name, 'planDigest': plan['digest']})
    info = json.loads(docker('inspect', cid))[0]
    controls = info['HostConfig']
    if (controls['NetworkMode'] != 'none' or controls.get('PortBindings') or controls['Privileged'] or
            not controls['ReadonlyRootfs'] or controls['Memory'] != b['memoryBytes'] or
            controls['MemorySwap'] != b['memoryBytes'] or controls['NanoCpus'] != b['cpus']*10**9 or
            controls['PidsLimit'] != b['pidsMax'] or controls['CapDrop'] != ['ALL']):
        raise ValueError('Created Docker controls differ from plan; container was not started')
    publish(workspace/'container-created.json', info)
    docker('start', cid)
    return {'state': 'dispatched', 'containerId': cid, 'workspace': str(workspace),
            'planDigest': plan['digest'], 'dataCorrectnessAccepted': False,
            'nextActions': [{'kind': 'read', 'argv': [sys.executable, str(Path(__file__).resolve()),
                             'status', '--plan', str(Path(plan_file).resolve())]}]}


def cgroup_controls():
    root = Path('/sys/fs/cgroup')
    values = {name: (root/name).read_text().strip() for name in
              ['cpu.max', 'memory.max', 'memory.current', 'memory.events', 'memory.swap.max',
               'pids.max', 'pids.current', 'pids.events']}
    for field in ['memory.events', 'pids.events']:
        values[field] = {k: int(v) for k, v in (row.split() for row in values[field].splitlines())}
    return values


def check_controls(values, budget):
    quota, period = map(int, values['cpu.max'].split())
    if (quota != period*budget['cpus'] or int(values['memory.max']) != budget['memoryBytes'] or
            int(values['memory.swap.max']) != 0 or int(values['pids.max']) != budget['pidsMax']):
        raise ValueError('Effective container cgroup limits differ')
    if values['pids.events'].get('max', 0) or any(values['memory.events'].get(k, 0)
                                                for k in ['oom', 'oom_kill', 'oom_group_kill']):
        raise RuntimeError('PID allocation or memory OOM event')


def import_counts(text):
    matches = re.findall(r'(?m)^Imported:\s*\n\s*([0-9,]+) nodes\s*\n\s*([0-9,]+) relationships\s*\n\s*([0-9,]+) properties', text)
    if len(matches) != 1:
        return None
    return dict(zip(['nodes', 'relationships', 'properties'], (int(x.replace(',', '')) for x in matches[0])))


def supervise_process(command, work, max_log_bytes, guard, child_record='import-child.json'):
    """Bound process output and guard latency; always terminate the owned process group."""
    guard()
    child = None
    log_bytes = 0
    try:
        with (work/'stdout.log').open('xb') as out, (work/'stderr.log').open('xb') as err:
            child = subprocess.Popen(command, cwd=work, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     start_new_session=True)
            publish(work/child_record, {'pid': child.pid, 'atUnix': time.time()})
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ, out)
                selector.register(child.stderr, selectors.EVENT_READ, err)
                while selector.get_map() or child.poll() is None:
                    guard()
                    for key, _ in selector.select(0.2):
                        raw = os.read(key.fileobj.fileno(), 65536)
                        if not raw:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                        else:
                            if log_bytes+len(raw) > max_log_bytes:
                                raise RuntimeError('Combined child stdout/stderr budget exceeded')
                            key.data.write(raw)
                            key.data.flush()
                            log_bytes += len(raw)
            code = child.wait(timeout=5)
        return code
    finally:
        if child is not None:
            try:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=10)
                # A leader can exit before its descendants; the group still belongs to this attempt.
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            finally:
                child.stdout.close()
                child.stderr.close()


def container_worker(plan_file):
    global STOP
    plan = load_plan(plan_file)
    if (os.environ.get('HGBENCH_REFERENCE_CONTAINER') != plan['digest'] or
            Path(plan_file) != Path('/control/plan.json')):
        raise ValueError('Internal worker requires its plan-bound container context')
    b, work = plan['budget'], Path('/work')
    started, next_guard = time.monotonic(), 0
    error, code, reported, verified_inputs = None, None, None, False
    def stop(*_):
        global STOP
        STOP = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    def guard(force=False):
        nonlocal next_guard
        now = time.monotonic()
        if STOP:
            raise RuntimeError('Import stop requested')
        if now-started >= b['maxSeconds']:
            raise TimeoutError('Whole import deadline exhausted')
        if not force and now < next_guard:
            return
        allocated = sum(p.stat().st_blocks*512 for root in [work, Path('/control')]
                        for p in root.rglob('*') if p.is_file() and not p.is_symlink())
        free = shutil.disk_usage(work).free
        actual = cgroup_controls()
        check_controls(actual, b)
        with (work/'resources.ndjson').open('a') as ledger:
            ledger.write(json.dumps({'atUnix': time.time(), 'allocatedBytes': allocated,
                                    'freeBytes': free, 'controls': actual})+'\n')
        if free < b['reserveFreeBytes'] or allocated > b['workspaceMaxBytes']:
            raise RuntimeError('Reference import disk budget exceeded')
        next_guard = now+5
    with (work/'worker.claim').open('x') as f:
        f.write(str(os.getpid())+'\n')
    try:
        guard(True)
        verify_files(ROOT, plan['componentFiles'], guard)
        facts = validate_manifest('/control/manifest.json', '/control/catalog.json',
                                  '/control/inventory.json', '/input')
        if facts != plan['facts']:
            raise ValueError('Staged reference metadata changed')
        if Path('/control/conf/neo4j.conf').read_text() != NEO4J_CONFIG:
            raise ValueError('Private import configuration changed')
        runtime_files = [{**x, 'path': str(PurePosixPath(x['path']).relative_to('neo4j-community-4.4.24'))}
                         for x in plan['runtimeFiles']]
        verify_tree('/neo4j', runtime_files, plan['runtimeLinks'], guard)
        verify_tree('/jdk', plan['javaFiles'], plan['javaLinks'], guard)
        inputs_before = input_fingerprints('/input', plan['facts']['tables'])
        verify_files('/input', plan['facts']['tables'], guard)
        if inputs_before != input_fingerprints('/input', plan['facts']['tables']):
            raise ValueError('CSV files changed during verification')
        verified_inputs = True
        publish(work/'verified-inputs.json', {'planDigest': plan['digest'], 'atUnix': time.time(),
                                            'csvBytes': facts['csvBytes'], 'files': len(facts['tables']),
                                            'inputFingerprints': inputs_before})
        command = importer_arguments(plan)
        publish(work/'import-command.json', {'argv': command, 'shell': False, 'atUnix': time.time()})
        code = supervise_process(command, work, b["maxLogBytes"], guard)
        guard(True)
        if inputs_before != input_fingerprints('/input', plan['facts']['tables']):
            raise ValueError('CSV files changed during import')
        if code != 0:
            raise RuntimeError('Neo4j importer exited '+str(code))
        reported = import_counts((work/'stdout.log').read_text(errors='replace'))
        if reported is None or any(reported[k] != facts['expected'][k] for k in ['nodes', 'relationships']):
            raise RuntimeError('Importer completion counts are missing or differ from the full CSV inventory')
        report = work/'import.report'
        if report.exists() and report.stat().st_size:
            raise RuntimeError('Importer reported bad records; manual inspection required')
    except BaseException as exc:
        error = type(exc).__name__+': '+str(exc)
    finally:
        publish(work/'receipt.json', {'schemaVersion': 1, 'kind': 'sf100-reference-import-receipt',
            'planDigest': plan['digest'], 'finishedAtUnix': time.time(),
            'elapsedSeconds': time.monotonic()-started, 'inputsFullyVerified': verified_inputs,
            'processExitCode': code, 'reportedCounts': reported, 'error': error,
            'state': 'process-completed-requires-database-validation' if error is None else 'failed-no-replay',
            'dataCorrectnessAccepted': False, 'referenceAnswersGenerated': False, 'performanceValid': False})
    return 0 if error is None else 4


def inspect_attempt(plan_file, stop=False):
    plan = load_plan(plan_file)
    root = Path(plan['workspace'])
    record, _ = read_json(root/'container.json')
    if record.get('planDigest') != plan['digest']:
        raise ValueError('Container receipt belongs to another plan')
    info = json.loads(docker('inspect', record['id']))[0]
    labels = info['Config'].get('Labels', {})
    if (labels.get('hgbench.plan') != plan['digest'] or labels.get('hgbench.workspace') != str(root) or
            labels.get('hgbench.owner') != 'sf100-reference-import'):
        raise ValueError('Container ownership changed')
    if stop and info['State']['Running']:
        docker('stop', '--time', '20', record['id'], timeout=30)
        info = json.loads(docker('inspect', record['id']))[0]
    receipt_path = root/'output/receipt.json'
    receipt = read_json(receipt_path)[0] if receipt_path.exists() else None
    if receipt is not None and receipt.get('planDigest') != plan['digest']:
        raise ValueError('Import receipt identity changed')
    counts = receipt.get('reportedCounts') if receipt is not None else None
    accepted = (receipt is not None and receipt.get('schemaVersion') == 1 and
                receipt.get('kind') == 'sf100-reference-import-receipt' and
                receipt.get('state') == 'process-completed-requires-database-validation' and
                receipt.get('inputsFullyVerified') is True and receipt.get('error') is None and
                type(receipt.get('processExitCode')) is int and receipt['processExitCode'] == 0 and
                isinstance(counts, dict) and all(type(counts.get(k)) is int and
                    counts[k] == plan['facts']['expected'][k] for k in ['nodes', 'relationships']) and
                not info['State']['Running'] and not info['State']['OOMKilled'] and info['State']['ExitCode'] == 0)
    state = ('running' if info['State']['Running'] else 'completed-needs-database-validation' if accepted
             else 'created-not-started' if info['State']['Status'] == 'created'
             else 'interrupted-unknown' if receipt is None else 'failed-no-replay')
    return {'state': state, 'containerId': record['id'], 'containerState': info['State'], 'receipt': receipt,
            'importProcessAccepted': accepted, 'dataCorrectnessAccepted': False,
            'recovery': 'inspect-same-execution; never-replay-existing-workspace'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    plan = commands.add_parser('plan', help='Freeze complete metadata, runtime identities and explicit import budgets')
    helps = {'prepared': 'Complete reference-manifest.json; partial conversions are rejected',
             'catalog': 'Frozen official SF100 asset catalog used by the converter',
             'inventory': 'Verified full initial-archive inventory used by the converter',
             'runtime-lock': 'Verified Neo4j 4.4.24 distribution.lock.json; every runtime file is required',
             'runtime-root': 'Existing parent directory containing neo4j-community-4.4.24/',
             'java-home': 'Existing fixed JDK directory; its files and relative links are frozen',
             'budget': 'Explicit JSON CPU/memory/time/disk/log budgets; see docs/reference-import.md',
             'workspace': 'NEW owned output directory, separate from all inputs/runtimes; never replayed',
             'out': 'NEW plan JSON file; planning reads metadata/runtime files but no complete CSV scan'}
    for flag, help_text in helps.items():
        plan.add_argument('--'+flag, type=Path, required=True, help=help_text)
    plan.add_argument('--image', required=True, help='Existing content-addressed Docker image ID; no pull')
    for name, help_text in [('run', 'Dispatch one new isolated import; returns container identity without waiting'),
                            ('status', 'Inspect the same container and receipt; missing receipt is not success'),
                            ('stop', 'Stop only this plan-owned container; preserve all files'),
                            ('container-worker', 'Internal bounded supervisor; only valid inside its prepared container')]:
        command = commands.add_parser(name, help=help_text, description=help_text)
        command.add_argument('--plan', type=Path, required=True,
                             help='Frozen plan from the plan command; existing attempts use status/stop, never run again')
    args = parser.parse_args()
    try:
        if args.command == 'plan':
            value = make_plan(args)
            publish(args.out, value)
            result = {'plan': str(args.out.resolve()), 'digest': value['digest'],
                      'expected': value['facts']['expected'], 'csvBytes': value['facts']['csvBytes'], 'executed': False}
        elif args.command == 'run':
            result = dispatch(args.plan)
        elif args.command == 'container-worker':
            return container_worker(args.plan)
        else:
            result = inspect_attempt(args.plan, args.command == 'stop')
        print(json.dumps({'ok': True, 'command': args.command, 'data': result}))
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(json.dumps({'ok': False, 'command': args.command, 'error': type(error).__name__, 'message': str(error)[:2048]}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
