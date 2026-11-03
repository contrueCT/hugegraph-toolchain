#!/usr/bin/env python3
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
"""Apply the fixed reference schema once, after full SF100 import; never retry a write."""
import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import tempfile
import time
import urllib.parse
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('reference_schema_verifier', ROOT/'tools/ldbc-reference-verify.py')
verifier = importlib.util.module_from_spec(spec); spec.loader.exec_module(verifier)
imp = verifier.imp
MAIN = 'org.apache.hugegraph.benchmark.reference.ReferenceSchemaMain'
SOURCE = ROOT/'adapters/ldbc-reference/schema-src'/Path(*MAIN.split('.')).with_suffix('.java')
SCHEMA = ROOT/'adapters/ldbc-reference/schema/neo4j-4.4.cypher'
SCHEMA_SHA = 'b49be47b68160a25e640c8f52cfa6423e7cecb50fc893a1522fe516d76792a3f'


def build(runtime, output):
    runtime, output = Path(runtime).resolve(), Path(output).resolve()
    verified, runtime_sha = verifier.runtime_files(runtime)
    dependencies = [runtime/'reference-verifier.jar'] + [Path(f['path']) for f in verified['dependencies']]
    tracked = imp.tracked(ROOT, [SOURCE, Path(__file__).resolve()])
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix='hgbench-reference-schema-') as classes:
        command = ['javac', '-J-Xmx256m', '-J-XX:ActiveProcessorCount=2', '--release', '17', '-encoding', 'UTF-8',
                   '-cp', os.pathsep.join(map(str, dependencies)), '-d', classes, str(SOURCE)]
        verifier.contract.build.bounded(command, output/'compile.log')
        with zipfile.ZipFile(output/'reference-schema.jar', 'x', compression=zipfile.ZIP_DEFLATED) as jar:
            for p in sorted(Path(classes).rglob('*.class')):
                entry = zipfile.ZipInfo(str(p.relative_to(classes)), date_time=(1980, 1, 1, 0, 0, 0))
                entry.external_attr = 0o100644 << 16; entry.compress_type = zipfile.ZIP_DEFLATED
                jar.writestr(entry, p.read_bytes())
            for name in ['LICENSE', 'NOTICE']:
                jar.writestr('META-INF/'+name, (ROOT/name).read_bytes())
    if tracked != imp.tracked(ROOT, [SOURCE, Path(__file__).resolve()]):
        raise ValueError('Schema component source changed while building')
    result = {'schemaVersion': 1, 'kind': 'reference-schema-runtime', 'mainClass': MAIN,
              'sourceFiles': tracked, 'files': imp.tracked(output, [output/'reference-schema.jar']),
              'verifierRuntime': str(runtime), 'verifierRuntimeSha256': runtime_sha,
              'dependencies': [{'path': str(p), 'bytes': p.stat().st_size, 'sha256': imp.file_hash(p)}
                               for p in dependencies], 'schemaSha256': SCHEMA_SHA,
              'javacVersion': subprocess.check_output(['javac', '-version'], text=True).strip(),
              'databaseExecuted': False, 'performanceAccepted': False}
    imp.publish(output/'build-manifest.json', result)
    return result


def runtime_files(directory):
    directory = Path(directory).resolve()
    value, digest = imp.read_json(directory/'build-manifest.json')
    if (value.get('kind') != 'reference-schema-runtime' or value.get('mainClass') != MAIN or
            value.get('schemaSha256') != SCHEMA_SHA):
        raise ValueError('Unexpected schema component runtime')
    imp.verify_files(directory, value['files']); imp.verify_files(ROOT, value['sourceFiles'])
    _, verification_sha = verifier.runtime_files(value['verifierRuntime'])
    if verification_sha != value['verifierRuntimeSha256']:
        raise ValueError('Underlying verifier runtime changed')
    for f in value['dependencies']:
        p = Path(f['path'])
        if not p.is_absolute() or p.is_symlink() or p.stat().st_size != f['bytes'] or imp.file_hash(p) != f['sha256']:
            raise ValueError('Schema dependency changed')
    return value, digest


def limits(value):
    if not isinstance(value, dict) or set(value) != {'maxSeconds', 'querySeconds', 'reserveFreeBytes'}:
        raise ValueError('Expected schema deadline, per-statement timeout and disk reserve')
    for name, low, high in [('maxSeconds', 1, 86400), ('querySeconds', 1, 3600),
                            ('reserveFreeBytes', 0, 1024**5)]:
        imp.integer(value[name], name, low, high)
    return value


def validate_binding(observed, window, imported, database, budget):
    identity = observed.get('databaseInfo')
    if (not isinstance(identity, dict) or set(identity) != {'id', 'name', 'creationDate'} or
            any(not isinstance(s, str) or not s or len(s) > 1024 for s in identity.values()) or
            database != 'neo4j' or identity['name'] != database or
            type(observed.get('nodes')) is not int or type(observed.get('relationships')) is not int or
            observed['nodes'] != imported['facts']['expected']['nodes'] or
            observed['relationships'] != imported['facts']['expected']['relationships'] or
            observed.get('settings', {}).get('dbms.read_only') != 'false' or observed.get('constraints') != [] or
            not isinstance(observed.get('indexes'), list)):
        raise ValueError('Schema requires the same complete, fresh, writable reference database')
    if any(not isinstance(row, dict) or row.get('type') != 'LOOKUP' or row.get('state') != 'ONLINE'
           for row in observed['indexes']):
        raise ValueError('Fresh reference database has unplanned indexes')
    if (window.get('kind') != 'reference-schema-writer-window' or window.get('databaseInfo') != identity or
            window.get('importPlanDigest') != imported['digest'] or
            any(window.get(k) is not True for k in
                ['exclusiveSchemaWriter', 'otherWritersStopped', 'inflightWritesDrained'])):
        raise ValueError('Missing schema-only exclusive writer window')
    until = datetime.datetime.fromisoformat(window['validUntil'].replace('Z', '+00:00'))
    if until.tzinfo is None or until <= datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=budget['maxSeconds']):
        raise ValueError('Writer window must cover the whole schema execution')
    return identity


def plan(args):
    imported = imp.load_plan(args.import_plan)
    status = imp.inspect_attempt(args.import_plan)
    if status['importProcessAccepted'] is not True or status['containerState']['Running']:
        raise ValueError('Complete strict reference import must be accepted first')
    facts = imp.validate_manifest(imported['metadata']['manifest'], imported['metadata']['catalog'],
                                  imported['metadata']['inventory'])
    if facts != imported['facts'] or facts['expected']['nodes'] != 282637871 or facts['expected']['relationships'] != 1775513741:
        raise ValueError('Expected unchanged complete SF100 conversion')
    observed, inspection_sha = imp.read_json(args.inspection)
    window, window_sha = imp.read_json(args.writer_window)
    budget = limits(imp.read_json(args.budget)[0])
    identity = validate_binding(observed, window, imported, args.database, budget)
    parsed = urllib.parse.urlsplit(args.uri)
    if (parsed.scheme != 'bolt' or parsed.hostname not in {'127.0.0.1', 'localhost', '::1'} or not parsed.port or
            parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment):
        raise ValueError('Use an isolated loopback Bolt URI')
    runtime, runtime_sha = runtime_files(args.runtime)
    if imp.file_hash(SCHEMA) != SCHEMA_SHA:
        raise ValueError('Fixed reference schema changed')
    return {'schemaVersion': 1, 'kind': 'reference-schema-plan', 'scale': '100', 'mainClass': MAIN,
            'database': args.database, 'databaseInfo': identity, 'uri': args.uri,
            'expected': facts['expected'], 'manifestSha256': facts['manifestSha256'],
            'importPlanDigest': imported['digest'], 'importReceiptSha256': imp.sha(imp.encoded(status['receipt'])),
            'inspectionSha256': inspection_sha, 'writerWindowSha256': window_sha, 'writerWindow': window,
            'initialInspection': observed, 'budget': budget, 'schemaFile': str(SCHEMA), 'schemaSha256': SCHEMA_SHA,
            'runtimeRoot': str(args.runtime.resolve()), 'runtimeManifestSha256': runtime_sha,
            'runtimeClasspath': [str(args.runtime.resolve()/'reference-schema.jar')] +
                                [f['path'] for f in runtime['dependencies']],
            'databaseWrites': 'fixed-schema-only', 'automaticWriteRetry': False,
            'requiresReadOnlyRestart': True, 'performanceAccepted': False}


def allocated(directory):
    total = 0; seen = set()
    for p in Path(directory).rglob('*'):
        try:
            st = p.lstat()
        except FileNotFoundError:
            continue
        if p.is_symlink():
            raise ValueError('Unexpected symlink in owned output')
        key = st.st_dev, st.st_ino
        if stat.S_ISREG(st.st_mode) and key not in seen:
            seen.add(key); total += st.st_blocks*512
    return total


def execute(args):
    started = time.monotonic()
    value, plan_sha = imp.read_json(args.plan)
    imported = imp.load_plan(args.import_plan)
    if (value.get('schemaVersion') != 1 or value.get('kind') != 'reference-schema-plan' or
            value.get('mainClass') != MAIN or value.get('scale') != '100' or
            value.get('schemaFile') != str(SCHEMA) or value.get('schemaSha256') != SCHEMA_SHA or
            value.get('databaseWrites') != 'fixed-schema-only' or value.get('automaticWriteRetry') is not False or
            value.get('importPlanDigest') != imported['digest'] or value.get('expected') != imported['facts']['expected'] or
            value.get('manifestSha256') != imported['facts']['manifestSha256']):
        raise ValueError('Schema execution differs from the accepted complete import')
    status = imp.inspect_attempt(args.import_plan)
    if (status['importProcessAccepted'] is not True or status['containerState']['Running'] or
            value['importReceiptSha256'] != imp.sha(imp.encoded(status['receipt']))):
        raise ValueError('Strict import acceptance changed')
    limits(value['budget'])
    validate_binding(value['initialInspection'], value['writerWindow'], imported, value['database'], value['budget'])
    runtime, runtime_sha = runtime_files(value['runtimeRoot'])
    classpath = [str(Path(value['runtimeRoot'])/'reference-schema.jar')] + [f['path'] for f in runtime['dependencies']]
    if classpath != value['runtimeClasspath'] or runtime_sha != value['runtimeManifestSha256'] or imp.file_hash(SCHEMA) != SCHEMA_SHA:
        raise ValueError('Schema runtime or DDL identity changed')
    resources = verifier.execution_resources(imp.read_json(args.resources)[0], 0)
    if os.name != 'posix' or not Path('/sys/fs/cgroup/cgroup.controllers').is_file() or os.getuid() == 0:
        raise ValueError('Execute in a non-root cgroup-v2 Linux container; CPU/memory/log limits are required')
    imp.check_controls(imp.cgroup_controls(), resources)
    java = Path(imported['javaHome']); database_files = Path(imported['workspace'])
    for path in [ROOT, java, Path(value['runtimeRoot']), database_files] + [Path(p) for p in classpath]:
        if not os.statvfs(path).f_flag & os.ST_RDONLY:
            raise ValueError('Component/JDK/runtime and reference data observation mounts must be read-only: '+str(path))
    out = args.out.resolve(); out.mkdir(mode=0o700)
    evidence = out/'evidence'; evidence.mkdir()
    imp.publish(out/'plan.json', value); imp.publish(out/'resources.json', resources)
    execution_sha = imp.file_hash(out/'plan.json')
    deadline = started+value['budget']['maxSeconds']; next_sample = 0; handlers = {}; error = None; code = None

    def cancel(*_):
        raise RuntimeError('Schema cancelled; preserve unknown effects, no automatic replay')

    def guard(force=False):
        nonlocal next_sample
        now = time.monotonic()
        if now >= deadline:
            raise RuntimeError('Whole schema execution deadline reached')
        if not force and now < next_sample:
            return
        next_sample = now+5
        controls = imp.cgroup_controls(); imp.check_controls(controls, resources)
        free = shutil.disk_usage(out).free
        db_bytes, output_bytes = allocated(database_files), allocated(out)
        if (free < max(value['budget']['reserveFreeBytes'], imported['budget']['reserveFreeBytes']) or
                output_bytes > resources['workspaceMaxBytes'] or db_bytes > imported['budget']['workspaceMaxBytes']):
            raise RuntimeError('Schema/index database or evidence disk budget reached')
        if imp.file_hash(args.plan) != plan_sha:
            raise ValueError('Schema source plan changed during execution')
        with (evidence/'resources.ndjson').open('a') as f:
            f.write(json.dumps({'atUnix': time.time(), 'controls': controls, 'freeBytes': free,
                               'databaseAllocatedBytes': db_bytes, 'evidenceAllocatedBytes': output_bytes})+'\n')

    try:
        for signum in [signal.SIGTERM, signal.SIGINT]:
            handlers[signum] = signal.signal(signum, cancel)
        guard(True)
        imp.verify_tree(java, imported['javaFiles'], imported['javaLinks'], guard)
        command = [str(java/'bin/java'), '-Xmx'+str(resources['heapMiB'])+'m',
                   '-XX:ActiveProcessorCount='+str(resources['cpus']), '-XX:+ExitOnOutOfMemoryError',
                   '-cp', os.pathsep.join(classpath), MAIN, '--plan', str(out/'plan.json'),
                   '--plan-sha256', execution_sha, '--out', str(out/'result')]
        imp.publish(evidence/'command.json', {'argv': command, 'shell': False, 'sourcePlanSha256': plan_sha})
        code = imp.supervise_process(command, evidence, resources['maxLogBytes'], guard,
                                     child_record='schema-child.json')
        guard(True)
        result, _ = imp.read_json(out/'result/result.json')
        if (code != 0 or (out/'result/failure.json').exists() or result.get('kind') != 'reference-schema-result' or
                result.get('schemaSha256') != SCHEMA_SHA or result.get('statementsAcknowledged') != 17 or
                result.get('schemaApplied') is not True or result.get('allIndexesOnline') is not True or
                imp.file_hash(out/'result/plan.json') != execution_sha):
            raise ValueError('Incomplete schema application or online-index proof')
    except BaseException as exc:
        error = type(exc).__name__+': '+str(exc)
    finally:
        for signum, old in handlers.items():
            signal.signal(signum, old)
        receipt = {'kind': 'reference-schema-execution', 'sourcePlanSha256': plan_sha,
                   'executionPlanSha256': execution_sha, 'processExitCode': code, 'error': error,
                   'state': 'schema-ready-restart-readonly' if error is None else 'failed-no-replay',
                   'schemaAppliedAndOnline': error is None, 'requiresReadOnlyRestart': True,
                   'dataCorrectnessAccepted': False, 'performanceAccepted': False,
                   'elapsedSeconds': time.monotonic()-started}
        imp.publish(out/'receipt.json', receipt)
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest='action', required=True)
    make = commands.add_parser('build', help='Offline Java 17 build, reusing the verified reference driver runtime')
    make.add_argument('--runtime', type=Path, required=True, help='Existing reference-verifier runtime; no downloads')
    make.add_argument('--out', type=Path, required=True, help='NEW small runtime directory')
    freeze = commands.add_parser('plan', help='Bind accepted full SF100 import, fresh DB identity and schema writer window')
    for name in ['import-plan', 'inspection', 'writer-window', 'budget', 'runtime', 'out']:
        freeze.add_argument('--'+name, type=Path, required=True)
    freeze.add_argument('--uri', required=True, help='Isolated loopback bolt:// endpoint; no credentials in URI')
    freeze.add_argument('--database', default='neo4j')
    run = commands.add_parser('execute', help='WRITE fixed schema once in an externally owned Linux container; no DB lifecycle',
        epilog='Writes 11 constraints + 6 indexes. Unknown commit stops without retry. Restart DB read-only before full verification.')
    for name in ['plan', 'import-plan', 'resources', 'out']:
        run.add_argument('--'+name, type=Path, required=True)
    args = p.parse_args()
    if args.action == 'build':
        result = build(args.runtime, args.out)
    elif args.action == 'plan':
        result = plan(args); imp.publish(args.out, result)
    else:
        result = execute(args)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get('state') != 'failed-no-replay' else 4


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError, KeyError) as error:
        print(json.dumps({'error': type(error).__name__, 'message': str(error), 'automaticWriteRetry': False}))
        raise SystemExit(2)
