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

"""Offline build of the pinned Cypher reference against the reviewed LDBC driver artifact.

Verifies source and dependency locks, applies the reviewed compatibility overlay, compiles upstream Java, copies the
29 locked queries, and packages a runtime. No Maven resolution or database calls.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / 'adapters/ldbc-reference'
IMPL_COMMIT = '11db98cc2ba14c33492f6c0c34e68c8be7e22e5f'
DRIVER_COMMIT = '8c6e849a60397eaced9f3cdd08b0efa430366cb9'
DRIVER_SHA = 'edab7d720ea195ac75f02c86d171b7124557e48c2f9748f3fcc09794f5d832b7'


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            result.update(block)
    return result.hexdigest()


def verified(root, entry):
    relative = Path(entry['path'])
    path = root / relative
    if relative.is_absolute() or '..' in relative.parts or path.is_symlink() or not path.is_file() \
            or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError('Unsafe or missing source/dependency: ' + entry['path'])
    if path.stat().st_size != entry['bytes'] or digest(path) != entry['sha256']:
        raise ValueError('Source/dependency drift: ' + entry['path'])
    return path


def bounded(command, logfile, seconds=60):
    with logfile.open('x') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=seconds)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise ValueError('Build/check cancelled or timed out; see ' + str(logfile))
    if code:
        raise ValueError('Build/check failed; see ' + str(logfile))


def inputs(deps, driver):
    source = REF / 'upstream'
    lock = json.loads((source / 'sources.lock.json').read_text())
    if lock['commit'] != IMPL_COMMIT or lock['repository'] != 'ldbc/ldbc_snb_interactive_v1_impls':
        raise ValueError('Unexpected reference source identity')
    checked = [verified(source, e) for e in lock['files']]
    declared = {str(p.relative_to(source)) for p in checked}
    actual = {str(p.relative_to(source)) for p in source.rglob('*') if p.is_file()}
    if actual != declared | {'sources.lock.json'}:
        raise ValueError('Untracked reference source/resource files')
    dependencies = json.loads((REF / 'dependencies.lock.json').read_text())['files']
    for entry in dependencies:
        verified(deps, entry)
    if driver.is_symlink() or digest(driver) != DRIVER_SHA:
        raise ValueError('LDBC driver differs from the reviewed artifact; rebuild/review its identity explicitly')
    reference = ROOT / 'benchmarks/ldbc-snb-interactive-v1/reference'
    queries = []
    for entry in json.loads((reference / 'sources.lock.json').read_text())['files']:
        if entry['path'].startswith('impls/cypher/queries/'):
            if entry['commit'] != IMPL_COMMIT:
                raise ValueError('Query and reference code commits differ')
            queries.append(verified(reference, entry))
    if len(queries) != 29:
        raise ValueError('Expected all 29 pinned Cypher queries')
    overlay = json.loads((REF / 'compatibility-overlay.json').read_text())
    if overlay['sourceCommit'] != IMPL_COMMIT or overlay['driverCommit'] != DRIVER_COMMIT:
        raise ValueError('Compatibility overlay targets different sources')
    if digest(ROOT / overlay['basis']['path']) != overlay['basis']['sha256']:
        raise ValueError('Compatibility overlay driver contract drift')
    for entry in overlay['files']:
        path = source / entry['path']
        if str(path.relative_to(source)) not in declared or digest(path) != entry['originalSha256']:
            raise ValueError('Compatibility overlay source drift')
        patched = path.read_text()
        for change in entry['replacements']:
            if patched.count(change['old']) != change['occurrences']:
                raise ValueError('Compatibility overlay patch context differs')
            patched = patched.replace(change['old'], change['new'])
        if hashlib.sha256(patched.encode()).hexdigest() != entry['patchedSha256']:
            raise ValueError('Compatibility overlay output drift')
    return checked, dependencies, queries, overlay


def build(deps, driver, output):
    deps, driver, output = Path(deps).resolve(), Path(driver).resolve(), Path(output).resolve()
    source_files, dependencies, queries, overlay = inputs(deps, driver)
    java = sorted(p for p in source_files if p.suffix == '.java')
    output.mkdir()
    cp = os.pathsep.join([str(driver), *(str(deps / e['path']) for e in dependencies)])
    with tempfile.TemporaryDirectory(prefix='hgbench-reference-compile-') as temporary:
        effective = {p: p for p in java}
        for entry in overlay['files']:
            original = REF / 'upstream' / entry['path']
            patched = original.read_text()
            for change in entry['replacements']:
                patched = patched.replace(change['old'], change['new'])
            target = Path(temporary) / 'patched-source' / entry['path']
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(patched)
            effective[original] = target
        classes = Path(temporary) / 'classes'
        classes.mkdir()
        bounded(['javac', '-J-Xmx256m', '-J-XX:ActiveProcessorCount=2', '--release', '17', '-encoding', 'UTF-8',
                 '-cp', cp, '-d', str(classes), *map(str, effective.values())], output / 'compile.log')
        entries = {p.relative_to(classes).as_posix(): p for p in classes.rglob('*.class')}
        entries.update({'META-INF/LICENSE': REF / 'upstream/LICENSE.txt',
                        'META-INF/NOTICE': REF / 'upstream/NOTICE.txt',
                        'META-INF/HGBENCH-NOTICE': REF / 'NOTICE',
                        'META-INF/hgbench-compatibility-overlay.json': REF / 'compatibility-overlay.json'})
        with zipfile.ZipFile(output / 'ldbc-cypher-reference.jar', 'x', compression=zipfile.ZIP_DEFLATED) as jar:
            for name, path in sorted(entries.items()):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.external_attr = 0o100644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                jar.writestr(info, path.read_bytes())
    runtime = [e for e in dependencies if e['scope'] == 'runtime']
    shutil.copyfile(driver, output / 'ldbc-driver-standalone.jar')
    for entry in runtime:
        shutil.copyfile(deps / entry['path'], output / entry['path'])
    (output / 'queries').mkdir()
    for path in queries:
        shutil.copyfile(path, output / 'queries' / path.name)
    # Recheck inputs after compilation; do not publish a successful manifest for a changing source tree.
    inputs(deps, driver)
    paths = [output / 'ldbc-cypher-reference.jar', output / 'ldbc-driver-standalone.jar',
             *(output / e['path'] for e in runtime), *sorted((output / 'queries').iterdir())]
    manifest = {'schemaVersion': 1, 'component': 'ldbc-cypher-reference', 'compileRelease': 17,
                'sourceCommit': IMPL_COMMIT, 'sourceLockSha256': digest(REF / 'upstream/sources.lock.json'),
                'driverCommit': DRIVER_COMMIT, 'driverSha256': DRIVER_SHA,
                'dependencyLockSha256': digest(REF / 'dependencies.lock.json'),
                'builderSha256': digest(__file__), 'sourceFilesCompiled': len(java),
                'buildBinding': 'pinned-upstream-plus-reviewed-overlay-explicit-classpath-no-Maven-resolution',
                'compatibilityOverlaySha256': digest(REF / 'compatibility-overlay.json'),
                'compatibilityChanges': overlay['files'],
                'upstreamDeclaredLdbcDriver': '1.2.0', 'selectedLdbcDriverPomVersion': '1.3.0-SNAPSHOT',
                'javacVersion': subprocess.run(['javac', '-version'], text=True, capture_output=True,
                                               check=True, timeout=10).stdout.strip(),
                'javaVersion': subprocess.run(['java', '-version'], text=True, capture_output=True,
                                              check=True, timeout=10).stderr.strip().splitlines(),
                'files': [{'path': str(p.relative_to(output)), 'bytes': p.stat().st_size, 'sha256': digest(p)}
                          for p in paths],
                'runtimeClasspath': [str(p.relative_to(output)) for p in paths if p.suffix == '.jar'],
                'mainClass': 'org.ldbcouncil.snb.driver.Client',
                'databaseClass': 'org.ldbcouncil.snb.impls.workloads.cypher.interactive.CypherInteractiveDb',
                'requiredJvmOptions': ['-Dfile.encoding=UTF-8', '-Duser.timezone=UTC'],
                'queryDirectory': 'queries', 'operations': 29, 'databaseExecuted': False,
                'referenceAnswersGenerated': False, 'executionReady': False}
    (output / 'build-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return {'output': str(output), 'sourceFilesCompiled': len(java), 'queries': len(queries),
            'databaseExecuted': False, 'manifestSha256': digest(output / 'build-manifest.json')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dependencies', required=True, help='Directory holding jars named in dependencies.lock.json')
    parser.add_argument('--ldbc-driver', required=True, help='Reviewed complete driver JAR from the existing HG build')
    parser.add_argument('--out', required=True, help='NEW runtime directory; failed build logs are retained')
    args = parser.parse_args()
    try:
        print(json.dumps({'ok': True, 'data': build(args.dependencies, args.ldbc_driver, args.out)}))
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(json.dumps({'ok': False, 'error': str(error)}))
        raise SystemExit(2)


if __name__ == '__main__':
    main()
