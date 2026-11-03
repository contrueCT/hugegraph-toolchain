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

"""Check real reference/driver registration, parameters and row mappings without a graph database."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('reference_build', ROOT / 'tools/build-ldbc-reference.py')
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


def runtime_files(runtime):
    manifest = json.loads((runtime / 'build-manifest.json').read_text())
    if manifest['sourceCommit'] != build.IMPL_COMMIT or manifest['driverCommit'] != build.DRIVER_COMMIT:
        raise ValueError('Unexpected runtime source/driver')
    for key, path in [('sourceLockSha256', build.REF / 'upstream/sources.lock.json'),
                      ('dependencyLockSha256', build.REF / 'dependencies.lock.json'),
                      ('compatibilityOverlaySha256', build.REF / 'compatibility-overlay.json')]:
        if manifest[key] != build.digest(path):
            raise ValueError('Runtime requires rebuilding after reviewed inputs changed: ' + key)
    deps = json.loads((build.REF / 'dependencies.lock.json').read_text())['files']
    expected_jars = {'ldbc-cypher-reference.jar', 'ldbc-driver-standalone.jar'} | {
        e['path'] for e in deps if e['scope'] == 'runtime'}
    if set(manifest['runtimeClasspath']) != expected_jars or len(manifest['runtimeClasspath']) != len(expected_jars):
        raise ValueError('Unexpected or duplicated runtime classpath entries')
    for entry in manifest['files']:
        build.verified(runtime, entry)
    for entry in deps:
        if entry['scope'] == 'runtime':
            build.verified(runtime, entry)
    if build.digest(runtime / 'ldbc-driver-standalone.jar') != build.DRIVER_SHA:
        raise ValueError('Wrong LDBC driver artifact')
    expected_queries = set()
    source = ROOT / 'benchmarks/ldbc-snb-interactive-v1/reference'
    for entry in json.loads((source / 'sources.lock.json').read_text())['files']:
        if entry['path'].startswith('impls/cypher/queries/'):
            rewritten = {**entry, 'path': 'queries/' + Path(entry['path']).name}
            build.verified(runtime, rewritten)
            expected_queries.add(rewritten['path'])
    if len(expected_queries) != 29 or set(e['path'] for e in manifest['files']) != expected_jars | expected_queries:
        raise ValueError('Runtime package is incomplete')
    return manifest


def check(runtime, output):
    runtime, output = Path(runtime).resolve(), Path(output).resolve()
    manifest = runtime_files(runtime)
    output.mkdir()
    source = ROOT / 'adapters/ldbc-reference/test/ReferenceBindingCheck.java'
    tasks = ROOT / 'benchmarks/ldbc-snb-interactive-v1/tasks.json'
    cp = os.pathsep.join(str(runtime / name) for name in manifest['runtimeClasspath'])
    with tempfile.TemporaryDirectory(prefix='hgbench-reference-check-') as temp:
        build.bounded(['javac', '-J-Xmx256m', '-J-XX:ActiveProcessorCount=2', '--release', '17', '-encoding', 'UTF-8',
                       '-cp', cp, '-d', temp, str(source)], output / 'compile.log', 30)
        build.bounded(['java', '-Xmx256m', '-XX:ActiveProcessorCount=2', '-Dfile.encoding=UTF-8',
                       '-cp', temp + os.pathsep + cp, 'org.apache.hugegraph.benchmark.reference.ReferenceBindingCheck',
                       str(runtime / 'queries'), str(tasks), str(output / 'result.json')], output / 'run.log', 30)
    runtime_files(runtime)
    report = json.loads((output / 'result.json').read_text())
    expected_tasks = json.loads(tasks.read_text())['tasks']
    if set(report['operations']) != {t['id'] for t in expected_tasks} or report['registeredOperations'] != 29 \
            or report['readResultMappingCount'] != 21 or report['databaseConnections'] != 0:
        raise ValueError('Incomplete runtime contract report')
    execution_jvm = report.get('executionJvm')
    if not isinstance(execution_jvm, dict) or any(not isinstance(execution_jvm.get(key), str)
            or not execution_jvm[key] for key in ('javaVersion', 'runtimeVersion', 'vendor', 'vmName', 'javaHome')):
        raise ValueError('Missing JVM identity from the process that executed the checks')
    report.update({'evidenceSchemaVersion': 2,
                   'runtimeManifestSha256': build.digest(runtime / 'build-manifest.json'),
                   'checkSourceSha256': build.digest(source), 'taskContractSha256': build.digest(tasks),
                   'wrapperSha256': build.digest(__file__), 'runtime': str(runtime),
                   'compileRelease': manifest['compileRelease'], 'buildJavaVersion': manifest['javaVersion'],
                   'buildJavacVersion': manifest['javacVersion'], 'checkCompileRelease': 17})
    (output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    return {'summary': str(output / 'summary.json'), 'operations': 29, 'readResultMappings': 21,
            'databaseExecuted': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', required=True, help='Built, verified reference runtime directory')
    parser.add_argument('--out', required=True, help='NEW directory for logs and the contract report')
    args = parser.parse_args()
    try:
        print(json.dumps({'ok': True, 'data': check(args.runtime, args.out)}))
    except (OSError, ValueError, KeyError) as error:
        print(json.dumps({'ok': False, 'error': str(error)}))
        raise SystemExit(2)


if __name__ == '__main__':
    main()
