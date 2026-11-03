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

"""Exercise the actual official validator and strict entry point with a no-DB protocol stub.

Uses existing hand-authored transport vectors. These checks establish exit and
evidence behavior, never graph correctness, SF100 execution, or performance.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MAIN = 'org.apache.hugegraph.benchmark.ldbc.ValidateDatabaseMain'
DB = 'org.apache.hugegraph.benchmark.ldbc.ValidationFixtureDb'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require(value, message):
    if not value:
        raise ValueError(message)


def run(args):
    runtime = args.runtime.resolve(); out = args.out.resolve()
    manifest = json.loads((runtime/'build-manifest.json').read_text())
    require(manifest['kind'] == 'official-ldbc-validation-launcher', 'Wrong launcher runtime')
    for entry in manifest['files']:
        require(sha(runtime/entry['path']) == entry['sha256'], 'Launcher runtime drift')
    driver = Path(manifest['driverJar']['path'])
    require(sha(driver) == manifest['driverJar']['sha256'], 'Official driver drift')
    require(sha(ROOT/manifest['source']['path']) == manifest['source']['sha256'], 'Launcher source drift')
    fixture = json.loads(args.vectors.read_text())
    require(fixture['scope'].startswith('unit-'), 'Only hand-authored unit vectors are accepted by this contract')
    task_file = ROOT/'benchmarks/ldbc-snb-interactive-v1/tasks.json'
    tasks = {t['driverType']: t for t in json.loads(task_file.read_text())['tasks']}
    template = ROOT/'adapters/ldbc-driver/validation/validation.properties'
    rows = []; types = []
    for vector in fixture['operations']:
        kind = vector['operationType']; task = tasks[kind]; op = {}
        for field in task['parameters']:
            name = key = field['name']
            if name.startswith(('personIdQ', 'personIdSQ')): key = 'personId'
            elif name.startswith('person1IdQ'): key = 'person1Id'
            elif name.startswith('person2IdQ'): key = 'person2Id'
            elif name.startswith('messageId') or name in {'messageForumId', 'messageRepliesId'}: key = 'messageId'
            elif name == 'locationIp': key = 'locationIP'
            op[field['constructorJsonName']] = vector['parameters'][key]
        result = '-1' if kind >= 1000 else (vector['result'][0] if task['resultShape'] == 'row' else vector['result'])
        row = '|'.join(json.dumps(v, ensure_ascii=False, separators=(',', ':')) for v in [op, result])
        require(row.count('|') == 1, 'Unit fixture contains an unsupported official CSV separator')
        rows.append(row+'\n'); types.append(kind)
    require(len(rows) == 44 and len(set(types)) == 29, 'Expected the existing 44-record/all-29-types transport fixture')
    out.mkdir()
    classes = out/'classes'; classes.mkdir()
    source = ROOT/'adapters/ldbc-contract/ValidationFixtureDb.java'
    compiled = subprocess.run(['javac', '-J-Xmx256m', '--release', '17', '-cp', str(driver),
                               '-d', str(classes), str(source)], capture_output=True, text=True, timeout=30)
    (out/'compile.log').write_text(compiled.stdout+compiled.stderr)
    require(compiled.returncode == 0, 'Fixture compilation failed; see compile.log')
    cp = os.pathsep.join(map(str, [runtime/'ldbc-validation.jar', driver, classes]))
    full = out/'full.csv'; full.write_text(''.join(rows))
    subset = out/'subset.csv'; subset.write_text(rows[0])
    empty = out/'empty.csv'; empty.touch()
    malformed = out/'malformed.csv'; malformed.write_text('invalid\n')
    # The pinned workload checks parameter filenames even in validate_database;
    # that mode never reads these files to generate operations.
    params = out/'unused-parameter-fixture'; params.mkdir()
    for i in range(1, 15):
        (params/('interactive_'+str(i)+'_param.txt')).touch()
    protected = out/'full-failed-actual.json'; protected.write_text('preserve original evidence')
    protected_expected = out/'full-failed-expected.json'; protected_expected.write_text('preserve expected evidence')
    full_sha = sha(full); records = []

    def one(name, mode='ok', validation=full, official=False, driver_mode='validate_database'):
        case = out/name; case.mkdir()
        if official:
            validation = case/'plain.csv'; validation.write_text(''.join(rows))
        props = {'database': DB, 'mode': driver_mode, 'validate_database': str(validation),
                 'fixture.reference': str(full), 'fixture.calls': str(case/'calls.log'), 'fixture.mode': mode,
                 'ldbc.snb.interactive.parameters_dir': str(params)}
        config = case/'driver.properties'
        config.write_text(''.join(k+'='+v.replace('\\', '\\\\')+'\n' for k, v in props.items()))
        argv = ['java', '-Xmx256m', '-XX:ActiveProcessorCount=2',
                '-Djdk.httpclient.disableRetryConnect=true', '-Djdk.httpclient.enableAllMethodRetry=false', '-cp', cp]
        if official:
            argv += ['org.ldbcouncil.snb.driver.Client']
        else:
            argv += [MAIN, '--out', str(case/'attempt'), '--']
        # The pinned driver gives earlier -P files priority over later files.
        argv += ['-P', str(config), '-P', str(template)]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=20, cwd=case)
        (case/'stdout.log').write_text(result.stdout); (case/'stderr.log').write_text(result.stderr)
        receipt_file = case/'attempt/receipt.json'
        receipt = json.loads(receipt_file.read_text()) if receipt_file.exists() else None
        records.append({'name': name, 'exitCode': result.returncode, 'argv': argv, 'receipt': receipt})
        return result, receipt, case

    original, _, plain_case = one('official-zero-on-wrong', 'wrong', official=True)
    require(original.returncode == 0, 'Pinned official Client regression no longer reproduced')
    failures = json.loads((plain_case/'plain-failed-actual.json').read_text())
    require(bool(failures), 'Official zero-exit regression did not actually contain wrong results')
    result, receipt, case = one('success')
    require(result.returncode == 0 and receipt['validationSuccessful'] and receipt['records'] == 44 and
            len(receipt['operationCounts']) == 29 and not receipt['performanceAccepted'], 'Complete success rejected')
    calls = (case/'calls.log').read_text().splitlines()
    require(calls == ['init', *map(str, types), 'close'], 'Official ordered replay changed')
    for mode in ['wrong', 'missing', 'throw', 'mutate']:
        result, receipt, case = one(mode, mode)
        require(result.returncode == 4 and receipt['officialModeInvoked'] and
                receipt['validationSuccessful'] is False, 'Invalid validation accepted: '+mode)
        if mode == 'missing':
            require(json.loads((case/'attempt/validation-failed-actual.json').read_text()) == [],
                    'Missing-handler case must demonstrate why an empty incorrect-results file is insufficient')
    for name, validation in [('subset', subset), ('empty', empty), ('malformed', malformed)]:
        result, receipt, case = one(name, validation=validation)
        require(result.returncode == 4 and receipt['validationSuccessful'] is False and
                receipt['officialModeInvoked'] is False and not (case/'calls.log').exists(),
                'Incomplete validation reached DB initialization: '+name)
    result, receipt, case = one('wrong-mode', driver_mode='execute_benchmark')
    require(result.returncode == 4 and not receipt['officialModeInvoked'] and not (case/'calls.log').exists(),
            'Wrong driver mode reached DB initialization')
    old = sha(out/'success/attempt/receipt.json')
    replay = subprocess.run(records[1]['argv'], capture_output=True, text=True, timeout=20)
    require(replay.returncode != 0 and sha(out/'success/attempt/receipt.json') == old, 'Existing attempt overwritten')
    require(sha(full) == full_sha and protected.read_text() == 'preserve original evidence' and
            protected_expected.read_text() == 'preserve expected evidence', 'Original reference files changed')
    report = {'checks': len(records)+2, 'records': 44, 'operationTypes': 29,
              'officialWrongResultExitCode': original.returncode, 'strictWrongResultExitCode': 4,
              'scope': 'Official JVM validator with a no-DB protocol stub; not graph correctness or SF100 performance',
              'databaseConnections': 0, 'performanceAccepted': False,
              'launcherManifestSha256': sha(runtime/'build-manifest.json'),
              'configurationTemplateSha256': sha(template),
              'fixtureSourceSha256': sha(source), 'vectorsSha256': sha(args.vectors), 'cases': records}
    (out/'checks.json').write_text(json.dumps(report, indent=2)+'\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', type=Path, required=True,
                        help='Verified launcher output from build-ldbc-validation.py')
    parser.add_argument('--vectors', type=Path, required=True, help='Existing hand-authored unit transport vectors')
    parser.add_argument('--out', type=Path, required=True, help='New evidence directory with every JVM output')
    args = parser.parse_args()
    report = run(args)
    print(json.dumps({k: v for k, v in report.items() if k != 'cases'}))


if __name__ == '__main__':
    main()
