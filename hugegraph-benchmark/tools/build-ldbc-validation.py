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

"""Offline build of a thin entry point for the pinned official ordered validator.

Uses an existing verified driver build. No Maven build, dependency resolution,
database connections, graph data or changes to the upstream driver.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('existing_driver_build', ROOT/'tools/build-ldbc-adapter.py')
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)
MAIN = 'org.apache.hugegraph.benchmark.ldbc.ValidateDatabaseMain'


def create(source, driver_build, output):
    source, driver_build, output = source.resolve(), driver_build.resolve(), output.resolve()
    source_sha = build.verified_source(source)
    build_manifest_sha = build.sha(driver_build/'build-manifest.json')
    manifest = json.loads((driver_build/'build-manifest.json').read_text())
    if (manifest.get('driverCommit') != build.COMMIT or
            manifest.get('driverSourceLockSha256') != source_sha):
        raise ValueError('Existing driver build does not match the pinned official source')
    matches = [f for f in manifest['files'] if f['path'] == 'ldbc-driver-standalone.jar']
    if len(matches) != 1:
        raise ValueError('Missing unique official standalone driver')
    driver = driver_build/'ldbc-driver-standalone.jar'
    if driver.is_symlink() or driver.stat().st_size != matches[0]['bytes'] or build.sha(driver) != matches[0]['sha256']:
        raise ValueError('Existing official driver JAR differs from its build manifest')
    java = ROOT/'adapters/ldbc-driver/validation/org/apache/hugegraph/benchmark/ldbc/ValidateDatabaseMain.java'
    java_sha = build.sha(java)
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix='hgbench-ldbc-validation-') as temp:
        build.bounded(['javac', '-J-Xmx256m', '--release', '17', '-encoding', 'UTF-8', '-cp', str(driver),
                       '-d', temp, str(java)], ROOT, output/'compile.log', 30)
        with zipfile.ZipFile(output/'ldbc-validation.jar', 'x', compression=zipfile.ZIP_DEFLATED) as jar:
            entries = {p.relative_to(temp).as_posix(): p for p in Path(temp).rglob('*.class')}
            entries.update({'META-INF/LICENSE': ROOT/'LICENSE', 'META-INF/NOTICE': ROOT/'NOTICE'})
            for name, path in sorted(entries.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.external_attr = 0o100644 << 16
                entry.compress_type = zipfile.ZIP_DEFLATED
                jar.writestr(entry, path.read_bytes())
    if (build.sha(java) != java_sha or build.sha(driver) != matches[0]['sha256'] or
            build.sha(driver_build/'build-manifest.json') != build_manifest_sha):
        raise ValueError('Launcher source, driver or build manifest changed during build')
    result = {'schemaVersion': 1, 'kind': 'official-ldbc-validation-launcher', 'mainClass': MAIN,
              'driverCommit': build.COMMIT, 'driverSourceLockSha256': source_sha,
              'driverBuildManifestSha256': build_manifest_sha,
              'driverJar': {**matches[0], 'path': str(driver)},
              'source': {'path': str(java.relative_to(ROOT)), 'sha256': java_sha}, 'javaRelease': 17,
              'files': [{'path': 'ldbc-validation.jar', 'bytes': (output/'ldbc-validation.jar').stat().st_size,
                         'sha256': build.sha(output/'ldbc-validation.jar')}],
              'javacVersion': subprocess.check_output(['javac', '-version'], text=True).strip(),
              'upstreamModified': False, 'databaseExecuted': False, 'performanceAccepted': False}
    (output/'build-manifest.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='Already fetched pinned official driver source')
    parser.add_argument('--driver-build', type=Path, required=True,
                        help='Existing HG adapter build manifest and driver JAR')
    parser.add_argument('--out', type=Path, required=True, help='New small launcher directory; no upstream rebuild')
    args = parser.parse_args()
    try:
        result = create(args.source, args.driver_build, args.out)
        print(json.dumps({'ok': True, 'output': str(args.out.resolve()), 'files': result['files'],
                          'databaseExecuted': False}))
        return 0
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(json.dumps({'ok': False, 'error': str(error)}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
