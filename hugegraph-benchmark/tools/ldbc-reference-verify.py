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

"""Build and plan full SF100 exact reference-database verification.

Uses the existing locked Java dependencies and no network resolution. Planning
requires an accepted strict import, the complete conversion and a frozen database
identity/lease. Java execution is read-only and must run under a bounded Docker task.
"""
import argparse
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
JAVA_ROOT = ROOT/'adapters/ldbc-reference/src'
MAIN = 'org.apache.hugegraph.benchmark.reference.ReferenceGraphVerifier'


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


imp = module('exact_reference_import', ROOT/'tools/ldbc-reference-import.py')
contract = module('exact_reference_runtime', ROOT/'tools/ldbc-reference-contract.py')


def build(runtime, output):
    runtime, output = Path(runtime).resolve(), Path(output).resolve()
    reference = contract.runtime_files(runtime)
    sources = sorted(JAVA_ROOT.rglob('*.java'))
    if {p.name for p in sources} != {'ExactReferenceMatcher.java', 'ReferenceGraphVerifier.java'}:
        raise ValueError('Unexpected verifier Java source inventory')
    deps = [runtime/name for name in reference['runtimeClasspath']]
    source_entries = imp.tracked(ROOT, sources)
    dep_entries = [{'path': str(p), 'bytes': p.stat().st_size, 'sha256': imp.file_hash(p)} for p in deps]
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix='hgbench-reference-verify-') as tmp:
        args = ['javac', '-J-Xmx256m', '-J-XX:ActiveProcessorCount=2', '--release', '17', '-encoding', 'UTF-8',
                '-cp', os.pathsep.join(map(str, deps)), '-d', tmp, *map(str, sources)]
        contract.build.bounded(args, output/'compile.log')
        with zipfile.ZipFile(output/'reference-verifier.jar', 'x', compression=zipfile.ZIP_DEFLATED) as jar:
            for source in sorted(Path(tmp).rglob('*.class')):
                entry = zipfile.ZipInfo(str(source.relative_to(tmp)), date_time=(1980, 1, 1, 0, 0, 0))
                entry.external_attr = 0o100644 << 16
                entry.compress_type = zipfile.ZIP_DEFLATED
                jar.writestr(entry, source.read_bytes())
            for name in ['LICENSE', 'NOTICE']:
                entry = zipfile.ZipInfo('META-INF/'+name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.external_attr = 0o100644 << 16
                entry.compress_type = zipfile.ZIP_DEFLATED
                jar.writestr(entry, (ROOT/name).read_bytes())
    if source_entries != imp.tracked(ROOT, sources):
        raise ValueError('Verifier sources changed during compilation')
    contract.runtime_files(runtime)
    result = {'schemaVersion': 1, 'kind': 'reference-verifier-runtime', 'mainClass': MAIN,
              'compileRelease': 17, 'sourceFiles': source_entries, 'dependencies': dep_entries,
              'files': imp.tracked(output, [output/'reference-verifier.jar']),
              'javacVersion': subprocess.check_output(['javac', '-version'], text=True).strip(),
              'databaseExecuted': False, 'performanceAccepted': False}
    imp.publish(output/'build-manifest.json', result)
    return result


def budget(value):
    fields = {'maxSeconds', 'querySeconds', 'reserveFreeBytes', 'maxRowBytes', 'maxBatchBytes', 'batchSize',
              'maxCandidatesPerRow', 'maxNodeId', 'maxRelationshipId'}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('Exact-verification budget fields differ')
    bounds = {'maxSeconds': (1, 172800), 'querySeconds': (1, 3600), 'reserveFreeBytes': (0, 16*1024**4),
              'maxRowBytes': (1, 4*1024**2), 'maxBatchBytes': (1, 64*1024**2), 'batchSize': (1, 10000),
              'maxCandidatesPerRow': (1, 10000), 'maxNodeId': (0, 2**31-1), 'maxRelationshipId': (0, 2**31-1)}
    for name, (low, high) in bounds.items():
        imp.integer(value[name], name, low, high)
    if value['maxBatchBytes'] < value['maxRowBytes'] or value['querySeconds'] > value['maxSeconds']:
        raise ValueError('Inconsistent verification time/byte budget')
    return value


def runtime_files(root):
    root = Path(root).resolve()
    value, raw_sha = imp.read_json(root/'build-manifest.json')
    if value.get('kind') != 'reference-verifier-runtime' or value.get('mainClass') != MAIN:
        raise ValueError('Unexpected verifier runtime')
    imp.verify_files(root, value['files'])
    imp.verify_files(ROOT, value['sourceFiles'])
    for entry in value['dependencies']:
        p = Path(entry['path'])
        if not p.is_absolute() or p.is_symlink() or p.stat().st_size != entry['bytes'] or imp.file_hash(p) != entry['sha256']:
            raise ValueError('Verifier dependency changed')
    return value, raw_sha


def check(runtime, output):
    runtime, output = Path(runtime).resolve(), Path(output).resolve()
    manifest, manifest_sha = runtime_files(runtime)
    output.mkdir()
    source = ROOT/'adapters/ldbc-reference/test/ExactReferenceMatcherCheck.java'
    source_sha = imp.file_hash(source)
    vectors = ROOT/'adapters/ldbc-reference/test/neo4j-4.4.24-csv-values.json'
    vector_sha = imp.file_hash(vectors)
    cp = os.pathsep.join([str(runtime/'reference-verifier.jar')] + [e['path'] for e in manifest['dependencies']])
    with tempfile.TemporaryDirectory(prefix='hgbench-reference-matcher-check-') as tmp:
        contract.build.bounded(['javac', '-J-Xmx256m', '-J-XX:ActiveProcessorCount=2', '--release', '17',
                                '-encoding', 'UTF-8', '-cp', cp, '-d', tmp, str(source)], output/'compile.log')
        contract.build.bounded(['java', '-Xmx256m', '-XX:ActiveProcessorCount=2', '-cp', tmp+os.pathsep+cp,
              'org.apache.hugegraph.benchmark.reference.ExactReferenceMatcherCheck', str(Path(tmp)/'output'), str(vectors)],
              output/'checks.log')
    results = json.loads((output/'checks.log').read_text())
    if results['databaseConnections'] != 0 or results['exhaustiveMultisetCases'] != 256 or results['nativeCsvCases'] != 24:
        raise ValueError('Matcher check scope changed')
    if imp.file_hash(source) != source_sha or imp.file_hash(vectors) != vector_sha:
        raise ValueError('Matcher tests changed during execution')
    result = {**results, 'runtimeManifestSha256': manifest_sha, 'testSourceSha256': source_sha, 'nativeCsvVectorSha256': vector_sha,
              'javaVersion': subprocess.run(['java', '-version'], text=True, capture_output=True,
                                           check=True, timeout=10).stderr.splitlines()}
    imp.publish(output/'result.json', result)
    return result


def plan(args):
    imported = imp.load_plan(args.import_plan)
    status = imp.inspect_attempt(args.import_plan)
    if status['importProcessAccepted'] is not True or status['containerState']['Running']:
        raise ValueError('Strict reference import has no accepted terminal receipt')
    facts = imp.validate_manifest(imported['metadata']['manifest'], imported['metadata']['catalog'],
                                  imported['metadata']['inventory'])
    if facts != imported['facts']:
        raise ValueError('Verification conversion differs from the accepted import')
    observed, inspection_sha = imp.read_json(args.inspection)
    identity = observed.get('databaseInfo')
    if (not isinstance(identity, dict) or set(identity) != {'id', 'name', 'creationDate'} or
            any(not isinstance(x, str) or not x or len(x) > 1024 for x in identity.values()) or
            identity['name'] != args.database or observed.get('nodes') != facts['expected']['nodes'] or
            observed.get('relationships') != facts['expected']['relationships']):
        raise ValueError('Database identity/count inspection differs from the full import')
    lease, lease_sha = imp.read_json(args.lease)
    if (lease.get('kind') != 'reference-writer-quiescence' or lease.get('importPlanDigest') != imported['digest'] or
            lease.get('databaseInfo') != identity or lease.get('writersStopped') is not True or
            lease.get('inflightWritesDrained') is not True or lease.get('exclusiveAccess') is not True):
        raise ValueError('Exclusive read-only reference lease does not match the import/database')
    limits = budget(imp.read_json(args.budget)[0])
    deadline = datetime.datetime.fromisoformat(lease['validUntil'].replace('Z', '+00:00'))
    if deadline.tzinfo is None or deadline <= datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=limits['maxSeconds']):
        raise ValueError('Lease must cover the complete frozen verification window')
    runtime, runtime_sha = runtime_files(args.runtime)
    specs, _ = imp.bridge().schema()
    tables = [{**t, 'columns': specs[t['table']]['columns']} for t in facts['tables']]
    import urllib.parse
    uri = urllib.parse.urlsplit(args.uri)
    if (uri.scheme != 'bolt' or uri.hostname not in {'127.0.0.1', 'localhost', '::1'} or not uri.port or
            uri.username or uri.password or uri.query or uri.fragment or uri.path):
        raise ValueError('Use an isolated loopback Bolt URI')
    result = {'schemaVersion': 1, 'kind': 'reference-exact-plan', 'scale': '100', 'database': args.database,
              'uri': args.uri, 'databaseInfo': identity, 'inputRoot': imported['inputRoot'],
              'tables': tables, 'expected': facts['expected'], 'budget': limits, 'leaseValidUntil': lease['validUntil'],
              'inspectionSha256': inspection_sha, 'leaseSha256': lease_sha, 'importPlanDigest': imported['digest'],
              'importReceiptSha256': imp.sha(imp.encoded(status['receipt'])),
              'manifestSha256': facts['manifestSha256'], 'runtimeManifestSha256': runtime_sha,
              'runtimeClasspath': [str(Path(args.runtime).resolve()/'reference-verifier.jar')] +
                                  [e['path'] for e in runtime['dependencies']],
              'mainClass': MAIN, 'databaseWrites': False, 'performanceAccepted': False,
              'requiredExternalControls': ['read-only CSV/runtime mounts', 'frozen owned Docker database identity',
                    'CPU/memory/output/log/deadline cgroup supervision', 'no concurrent writers or database replacement'],
              'bitmapLogicalBytes': (limits['maxNodeId']//8+1) + (limits['maxRelationshipId']//8+1)}
    if any(facts['expected'][k] > limits[f] + 1 for k, f in [('nodes', 'maxNodeId'), ('relationships', 'maxRelationshipId')]):
        raise ValueError('Bitmap cannot hold the complete expected graph')
    return result


def execution_resources(value, bitmap_bytes):
    fields = {'cpus', 'memoryBytes', 'heapMiB', 'pidsMax', 'workspaceMaxBytes', 'maxLogBytes'}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('Verifier execution-resource fields differ')
    for field, low, high in [('cpus', 1, 16), ('memoryBytes', 768*1024**2, 64*1024**3),
                             ('heapMiB', 256, 32768), ('pidsMax', 64, 1024),
                             ('workspaceMaxBytes', 1024**2, 16*1024**3),
                             ('maxLogBytes', 4096, 256*1024**2)]:
        imp.integer(value[field], field, low, high)
    if value['memoryBytes'] < value['heapMiB']*1024**2 + bitmap_bytes + 512*1024**2:
        raise ValueError('Memory must cover heap, complete bitmaps and 512 MiB native headroom')
    if value['workspaceMaxBytes'] < bitmap_bytes + value['maxLogBytes'] + 1024**2:
        raise ValueError('Workspace must cover complete bitmaps, logs and evidence')
    return value


def load_execution_plan(path, import_path):
    value, identity = imp.read_json(path)
    imported = imp.load_plan(import_path)
    if (value.get('schemaVersion') != 1 or value.get('kind') != 'reference-exact-plan' or
            value.get('scale') != '100' or value.get('mainClass') != MAIN or
            value.get('databaseWrites') is not False or value.get('importPlanDigest') != imported['digest'] or
            value.get('expected') != imported['facts']['expected'] or
            value.get('manifestSha256') != imported['facts']['manifestSha256'] or
            value.get('inputRoot') != imported['inputRoot']):
        raise ValueError('Verifier plan differs from the accepted strict import')
    limits = budget(value['budget'])
    until = datetime.datetime.fromisoformat(value['leaseValidUntil'].replace('Z', '+00:00'))
    if (until.tzinfo is None or until <= datetime.datetime.now(datetime.timezone.utc) +
            datetime.timedelta(seconds=limits['maxSeconds'])):
        raise ValueError('Writer lease no longer spans the whole execution window; make a new plan')
    specs, _ = imp.bridge().schema()
    tables = [{**t, 'columns': specs[t['table']]['columns']} for t in imported['facts']['tables']]
    if value.get('tables') != tables or len(tables) != 31:
        raise ValueError('Verifier execution requires all 31 unchanged CSV tables')
    bitmaps = (limits['maxNodeId']//8+1) + (limits['maxRelationshipId']//8+1)
    if value.get('bitmapLogicalBytes') != bitmaps:
        raise ValueError('Verifier bitmap identity differs')
    return value, identity, imported


def validate_result(directory, plan):
    result, result_sha = imp.read_json(directory/'result.json')
    expected = {t['table']: t for t in plan['tables']}
    tables = result.get('tables', [])
    if (result.get('schemaVersion') != 1 or result.get('kind') != 'reference-exact-verification' or
            result.get('allRowsCompared') is not True or result.get('referenceAnswersGenerated') is not False or
            result.get('performanceAccepted') is not False or
            type(result.get('nodesMatched')) is not int or type(result.get('relationshipsMatched')) is not int or
            result.get('nodesMatched') != plan['expected']['nodes'] or
            result.get('relationshipsMatched') != plan['expected']['relationships'] or
            not isinstance(tables, list) or len(tables) != 31 or
            len({t['table'] for t in tables}) != 31 or {t['table'] for t in tables} != set(expected)):
        raise ValueError('Java verifier lacks the complete full-graph bijection result')
    for index, t in enumerate(tables, 1):
        source = expected[t['table']]
        if (type(t.get('rows')) is not int or type(t.get('batches')) is not int or t['batches'] < 0 or
                t.get('rows') != source['rows'] or t.get('csvSha256') != source['sha256'] or
                not re.fullmatch('[a-f0-9]{64}', t.get('matchedInternalIdsSha256', '')) or
                not re.fullmatch('[a-f0-9]{64}', t.get('querySha256', '')) or
                imp.read_json(directory/('table-'+str(index)+'.json'))[0] != t):
            raise ValueError('Java table receipt differs from the complete frozen CSV inventory')
    before = imp.read_json(directory/'before.json')[0]
    after = imp.read_json(directory/'after.json')[0]
    if (before != after or before.get('databaseInfo') != plan['databaseInfo'] or
            type(before.get('nodes')) is not int or type(before.get('relationships')) is not int or
            before['nodes'] != plan['expected']['nodes'] or
            before['relationships'] != plan['expected']['relationships']):
        raise ValueError('Reference database identity/schema changed during verification')
    return result_sha


def execute(args):
    """Run inside an externally owned Linux container; never start/stop a DB."""
    started = time.monotonic()
    frozen, plan_sha, imported = load_execution_plan(args.plan, args.import_plan)
    resources = execution_resources(imp.read_json(args.resources)[0], frozen['bitmapLogicalBytes'])
    if os.name != 'posix' or not Path('/sys/fs/cgroup/cgroup.controllers').is_file() or os.getuid() == 0:
        raise ValueError('Run in a non-root Linux cgroup-v2 container with frozen read-only mounts')
    imp.check_controls(imp.cgroup_controls(), resources)
    runtime = Path(frozen['runtimeClasspath'][0]).parent
    actual, runtime_sha = runtime_files(runtime)
    classpath = [str(runtime/'reference-verifier.jar')] + [e['path'] for e in actual['dependencies']]
    if runtime_sha != frozen['runtimeManifestSha256'] or classpath != frozen['runtimeClasspath']:
        raise ValueError('Verifier runtime differs from the execution plan')
    java_home = Path(imported['javaHome'])
    # statvfs reports the mount's read-only flag, including Docker bind mounts.
    for path in [ROOT, Path(frozen['inputRoot']), java_home, runtime] + [Path(p) for p in classpath]:
        if not os.statvfs(path).f_flag & os.ST_RDONLY:
            raise ValueError('CSV, Java, component and verifier runtime must be mounted read-only: '+str(path))
    out = args.out.resolve()
    out.mkdir(mode=0o700)
    control = out/'control'; control.mkdir()
    evidence = out/'evidence'; evidence.mkdir()
    imp.publish(control/'plan.json', frozen)
    imp.publish(control/'resources.json', resources)
    # Java checks the exact bytes of its plan, rather than the original formatting.
    execution_sha = imp.file_hash(control/'plan.json')
    deadline = started + frozen['budget']['maxSeconds']
    next_sample = 0; code = None; error = None; result_sha = None; stopped = False
    old_handlers = {}

    def cancel(signum, frame):
        nonlocal stopped
        if not stopped:
            stopped = True
            # Selectors may consume InterruptedError as a retriable syscall;
            # use a normal exception so cancellation crosses that boundary.
            raise RuntimeError('Verification cancelled; keep partial evidence, no automatic replay')

    def guard(force=False):
        nonlocal next_sample
        now = time.monotonic()
        if stopped or now >= deadline:
            raise RuntimeError('Whole verifier execution deadline or cancellation reached')
        if not force and now < next_sample:
            return
        next_sample = now+5
        observed = imp.cgroup_controls()
        imp.check_controls(observed, resources)
        allocated = 0
        for path in out.rglob('*'):
            try:
                stat = path.lstat()
            except FileNotFoundError:
                continue
            if path.is_symlink():
                raise ValueError('Unexpected symlink in verifier evidence')
            allocated += stat.st_blocks*512
        free = shutil.disk_usage(out).free
        if free < frozen['budget']['reserveFreeBytes'] or allocated > resources['workspaceMaxBytes']:
            raise RuntimeError('Verifier disk budget reached')
        if imp.file_hash(args.plan) != plan_sha:
            raise ValueError('Original verification plan changed during execution')
        with (evidence/'resources.ndjson').open('a') as ledger:
            ledger.write(json.dumps({'atUnix': time.time(), 'allocatedBytes': allocated,
                                    'freeBytes': free, 'controls': observed})+'\n')

    try:
        for signum in [signal.SIGTERM, signal.SIGINT]:
            old_handlers[signum] = signal.signal(signum, cancel)
        guard(True)
        imp.verify_tree(java_home, imported['javaFiles'], imported['javaLinks'], guard)
        command = [str(java_home/'bin/java'), '-Xmx'+str(resources['heapMiB'])+'m',
                   '-XX:ActiveProcessorCount='+str(resources['cpus']), '-XX:+ExitOnOutOfMemoryError',
                   '-cp', os.pathsep.join(classpath), MAIN, 'verify', '--plan', str(control/'plan.json'),
                   '--plan-sha256', execution_sha, '--out', str(out/'result')]
        imp.publish(evidence/'command.json', {'argv': command, 'shell': False,
                    'sourcePlanSha256': plan_sha, 'executionPlanSha256': execution_sha})
        code = imp.supervise_process(command, evidence, resources['maxLogBytes'], guard,
                                     child_record='verifier-child.json')
        guard(True)
        if code != 0:
            raise RuntimeError('Java verifier exited '+str(code))
        if (out/'result/failure.json').exists() or imp.file_hash(out/'result/plan.json') != execution_sha:
            raise ValueError('Java verifier failure or mismatched plan receipt')
        result_sha = validate_result(out/'result', frozen)
        guard(True)
    except BaseException as exc:
        error = type(exc).__name__+': '+str(exc)
    finally:
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
        receipt = {'schemaVersion': 1, 'kind': 'reference-verifier-execution',
                   'sourcePlanSha256': plan_sha, 'executionPlanSha256': execution_sha,
                   'importPlanDigest': imported['digest'], 'runtimeManifestSha256': runtime_sha,
                   'state': 'verified' if error is None else 'failed-no-replay',
                   'processExitCode': code, 'error': error, 'elapsedSeconds': time.monotonic()-started,
                   'resultSha256': result_sha, 'dataCorrectnessAccepted': error is None,
                   'referenceAnswersGenerated': False, 'performanceAccepted': False}
        imp.publish(out/'receipt.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    compile_cmd = commands.add_parser('build', help='Offline Java 17-target build; no DB or dependency downloads')
    compile_cmd.add_argument('--runtime', type=Path, required=True, help='Previously verified Cypher reference runtime')
    compile_cmd.add_argument('--out', type=Path, required=True, help='New small verifier output directory')
    checks = commands.add_parser('check', help='Run exact matching, typed values, CSV and bitmap contract checks; no DB')
    checks.add_argument('--runtime', type=Path, required=True, help='Verifier output from build')
    checks.add_argument('--out', type=Path, required=True, help='New check evidence directory')
    make = commands.add_parser('plan', help='Freeze complete CSV/import/DB identity after strict import completed')
    for name, help_text in [('import-plan', 'Executed strict reference import plan; Docker status is checked'),
                            ('inspection', 'JSON from ReferenceGraphVerifier inspect against the actual read-only DB'),
                            ('lease', 'External reference-writer-quiescence lease for this exact import/database'),
                            ('budget', 'Explicit JSON verification budgets; see docs/reference-verification.md'),
                            ('runtime', 'Verifier output from build'), ('out', 'New immutable verification plan JSON')]:
        make.add_argument('--'+name, type=Path, required=True, help=help_text)
    make.add_argument('--uri', required=True, help='Isolated loopback Bolt URI shared through Docker network namespace')
    make.add_argument('--database', default='neo4j')
    run = commands.add_parser('execute', help='Run the frozen verifier in a bounded Linux container; no DB lifecycle',
                             description='Requires a read-only reference DB in the same private network namespace '
                                         'and non-root cgroup-v2 controls. CSV, Java, verifier runtime/dependencies '
                                         'and component sources must be read-only mounts. The external owner '
                                         'supplies Docker isolation and preserves the reference database. '
                                         'Partial evidence is kept on failure, cancellation or whole-task deadline; '
                                         'never reuse --out. No query retries.')
    run.add_argument('--plan', type=Path, required=True, help='Frozen full verification plan from plan')
    run.add_argument('--import-plan', type=Path, required=True,
                     help='The same strict import plan, including the fixed Java runtime inventory')
    run.add_argument('--resources', type=Path, required=True,
                     help='JSON: cpus, memoryBytes, heapMiB, pidsMax, workspaceMaxBytes, maxLogBytes; '
                          'see reference-verification.md in docs/')
    run.add_argument('--out', type=Path, required=True,
                     help='New writable evidence directory; Java creates its result/ subdirectory')
    args = parser.parse_args()
    try:
        if args.command == 'execute':
            result = execute(args)
            print(json.dumps(result))
            return 0 if result['dataCorrectnessAccepted'] else 4
        result = (build(args.runtime, args.out) if args.command == 'build' else
                  check(args.runtime, args.out) if args.command == 'check' else plan(args))
        if args.command == 'plan':
            imp.publish(args.out, result)
        target = (args.out/'build-manifest.json' if args.command == 'build' else
                  args.out/'result.json' if args.command == 'check' else args.out)
        print(json.dumps({'ok': True, 'command': args.command, 'output': str(target.resolve()),
                          'sha256': imp.file_hash(target), 'databaseExecuted': False}))
        return 0
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as e:
        print(json.dumps({'ok': False, 'error': type(e).__name__, 'message': str(e)[:2048]}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
