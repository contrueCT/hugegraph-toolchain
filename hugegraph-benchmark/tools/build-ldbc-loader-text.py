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

"""Build a small text-preserving native Loader overlay for exact LDBC imports.

Offline: reads one reviewed DataTypeUtil.java and existing Loader lib/*.jar;
compiles with an explicit JDK, checks original trim behavior and the final Jar,
and writes a new output directory. No database, graph data, Maven, downloads,
or edits to the supplied source/distribution. Does not enable HTTP retries.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
CLASS = 'org/apache/hugegraph/loader/util/DataTypeUtil.class'
ORIGINAL = ('        // Trim space if raw value is string\n'
            '        Object value = rawValue;\n'
            '        if (rawValue instanceof String) {')
REPLACEMENT = ('        // Preserve original LDBC text; typed scalar parsing still trims.\n'
               '        Object value = rawValue;\n'
               '        if (rawValue instanceof String && !dataType.isText()) {')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def transform(raw, reviewed_sha):
    if not re.fullmatch(r'[a-f0-9]{64}', reviewed_sha) or hashlib.sha256(raw).hexdigest() != reviewed_sha:
        raise ValueError('Loader source differs from the reviewed SHA-256')
    if len(raw) > 128 * 1024:
        raise ValueError('Loader source exceeds 128 KiB')
    source = raw.decode('utf-8')
    if source.count(ORIGINAL) != 1:
        raise ValueError('Expected unique parseSingleValue trim block; review this Loader version')
    return source.replace(ORIGINAL, REPLACEMENT).encode('utf-8')


def bounded(command, output, seconds):
    environment = dict(os.environ)
    for name in ['JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS']:
        environment.pop(name, None)
    with output.open('xb') as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                   env=environment, start_new_session=True)
        try:
            code = process.wait(timeout=seconds)
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    if code != 0 or output.stat().st_size > 4 * 1024**2:
        raise ValueError('Native Loader check failed; inspect ' + str(output))


def create(source, reviewed_sha, loader_home, jdk_home, output, loader_jar=None):
    source, loader_home, jdk_home, output = [path.resolve() for path in
                                           [source, loader_home, jdk_home, output]]
    if source.stat().st_size > 128 * 1024:
        raise ValueError('Loader source exceeds 128 KiB')
    patched = transform(source.read_bytes(), reviewed_sha)
    libraries = sorted((loader_home / 'lib').glob('*.jar'))
    if not libraries or any(not path.is_file() for path in libraries):
        raise ValueError('Existing Loader lib/*.jar required')
    implementations = []
    for path in libraries:
        with zipfile.ZipFile(path) as archive:
            if CLASS in archive.namelist():
                implementations.append({'path': path, 'classSha256': hashlib.sha256(archive.read(CLASS)).hexdigest()})
    if not implementations:
        raise ValueError('Missing native DataTypeUtil implementation')
    if loader_jar is None:
        if len({item['classSha256'] for item in implementations}) != 1:
            raise ValueError('Conflicting native DataTypeUtil implementations; select the actual --loader-jar')
        selected = implementations[0]['path']
    else:
        selected = loader_jar.resolve()
        if selected not in {item['path'] for item in implementations}:
            raise ValueError('Selected Loader Jar must contain the native class in loader-home/lib')
    libraries = [selected] + [path for path in libraries if path != selected]
    java, javac = jdk_home / 'bin/java', jdk_home / 'bin/javac'
    if not java.is_file() or not javac.is_file():
        raise ValueError('Explicit JDK with java and javac required')
    probe = ROOT / 'adapters/ldbc-loader/LoaderTextProbe.java'
    inputs = [{'path': str(path), 'bytes': path.stat().st_size, 'sha256': sha(path)}
              for path in libraries + [source, probe, Path(__file__).resolve()]]
    output.mkdir()
    try:
        patched_source = output / 'DataTypeUtil.java'
        patched_source.write_bytes(patched)
        classpath = os.pathsep.join(map(str, libraries))
        overlay = output / 'ldbc-loader-preserve-text.jar'
        with tempfile.TemporaryDirectory(prefix='compile-', dir=output) as temp:
            bounded([str(javac), '-J-Xmx256m', '-J-XX:ActiveProcessorCount=1', '--release', '17',
                     '-encoding', 'UTF-8', '-Xmaxerrs', '10',
                     '-cp', classpath, '-d', temp, str(patched_source), str(probe)], output / 'compile.log', 60)
            entries = {path.relative_to(temp).as_posix(): path
                       for path in (Path(temp) / 'org/apache/hugegraph/loader/util').glob('*.class')}
            if CLASS not in entries:
                raise ValueError('Missing compiled DataTypeUtil')
            entries.update({'META-INF/LICENSE': ROOT / 'LICENSE', 'META-INF/NOTICE': ROOT / 'NOTICE'})
            with zipfile.ZipFile(overlay, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
                for name, path in sorted(entries.items()):
                    entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                    entry.external_attr = 0o100644 << 16
                    entry.compress_type = zipfile.ZIP_DEFLATED
                    archive.writestr(entry, path.read_bytes())
            for mode, cp, expected in [('trim', classpath, selected),
                                       ('preserve', str(overlay) + os.pathsep + classpath, overlay)]:
                receipt = output / (mode + '.json')
                bounded([str(java), '-XX:ActiveProcessorCount=1', '-Xmx256m', '-cp', cp + os.pathsep + temp,
                         'LoaderTextProbe', mode, str(receipt), str(expected)], output / (mode + '.log'), 30)
                report = json.loads(receipt.read_text())
                if type(report.get('checks')) is not int or report['checks'] != 8 or report.get('mode') != mode:
                    raise ValueError('Native process exited without the complete behavioral receipt')
        for fact in inputs:
            path = Path(fact['path'])
            if path.stat().st_size != fact['bytes'] or sha(path) != fact['sha256']:
                raise ValueError('Source or dependency changed during build: ' + str(path))
        bounded([str(java), '-version'], output / 'java-version.log', 10)
        bounded([str(javac), '-version'], output / 'javac-version.log', 10)
        result = {'schemaVersion': 1, 'kind': 'ldbc-loader-text-overlay', 'javaRelease': 17,
                  'jdkHome': str(jdk_home),
                  'originalSourceSha256': reviewed_sha, 'patchedSourceSha256': sha(patched_source),
                  'nativeImplementations': [{**item, 'path': str(item['path'])} for item in implementations],
                  'selectedLoaderJar': str(selected),
                  'inputs': inputs, 'baselineChecks': 8, 'packagedOverlayChecks': 8,
                  'classpathPrefix': str(overlay), 'originalLoaderModified': False, 'databaseExecuted': False,
                  'performanceAccepted': False,
                  'files': [{'path': path.name, 'bytes': path.stat().st_size, 'sha256': sha(path)}
                            for path in [overlay, patched_source, output / 'trim.json', output / 'preserve.json']]}
        (output / 'build-manifest.json').write_text(json.dumps(result, indent=2) + '\n')
        return result
    except BaseException as error:
        (output / 'failure.json').write_text(json.dumps({'status': 'failed', 'error': str(error),
                                                       'databaseExecuted': False}) + '\n')
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='Matching native Loader DataTypeUtil.java')
    parser.add_argument('--source-sha256', required=True, help='Reviewed source digest from the frozen build')
    parser.add_argument('--loader-home', type=Path, required=True, help='Existing native Loader distribution with lib/')
    parser.add_argument('--loader-jar', type=Path,
                        help='Jar used by the native launcher; required when thin/shaded classes differ')
    parser.add_argument('--jdk-home', type=Path, required=True, help='Explicit JDK 17+; uses fixed JVM flags')
    parser.add_argument('--out', type=Path, required=True, help='New small output; existing paths are refused')
    args = parser.parse_args()
    try:
        result = create(args.source, args.source_sha256, args.loader_home, args.jdk_home, args.out, args.loader_jar)
        print(json.dumps({'ok': True, 'output': str(args.out.resolve()), 'classpathPrefix': result['classpathPrefix'],
                          'baselineChecks': 8, 'packagedOverlayChecks': 8, 'databaseExecuted': False}))
        return 0
    except (OSError, ValueError, KeyError, subprocess.SubprocessError, zipfile.BadZipFile) as error:
        print(json.dumps({'ok': False, 'error': str(error)}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
