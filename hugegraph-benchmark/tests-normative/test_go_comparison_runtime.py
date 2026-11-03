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
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1] / 'components/http'
SPEC = importlib.util.spec_from_file_location('go_runtime_test', HERE / 'go-comparison-runtime.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def executor():
    return {'engine': 'go-http-v1', 'clockProof': '/existing/clock', 'binary': {
        'path': '/explicit/runner', 'sha256': 'a' * 64, 'sourceSha256': 'b' * 64,
        'goVersion': 'go1.27.3', 'goos': 'linux', 'goarch': 'amd64'}}


class GoRuntimeContract(unittest.TestCase):
    def test_executor_and_profile_resources_are_explicit(self):
        value = executor()
        MOD.validate(value)
        for change in (lambda v: v.update(engine='node'), lambda v: v['binary'].update(goos='darwin'),
                       lambda v: v['binary'].update(sha256='bad'), lambda v: v['binary'].update(path='/source/hgbench'),
                       lambda v: v.update(clockProof='relative'), lambda v: v.update(extra='unexpected')):
            changed = json.loads(json.dumps(value))
            change(changed)
            with self.assertRaises(MOD.GoRuntimeError):
                MOD.validate(changed)
        runtime = {'executor': value, 'driver': {'cpus': 2, 'memoryBytes': 1073741824, 'python': '/usr/local/bin/python3'}}
        profile = {'schemaVersion': 2, 'executor': {'binary': value['binary'], 'hostIdentityGate': True,
                                                   'cpuQuotaCores': 2, 'maxRssBytes': 1073741824}}
        MOD.bind_profile(runtime, profile)
        for change in (lambda p: p['executor'].update(hostIdentityGate=False),
                       lambda p: p['executor'].update(cpuQuotaCores=1), lambda p: p.update(schemaVersion=1)):
            changed = json.loads(json.dumps(profile))
            change(changed)
            with self.assertRaises(MOD.GoRuntimeError):
                MOD.bind_profile(runtime, changed)

    def test_every_run_and_service_check_has_a_bounded_unique_location(self):
        p = {'cases': [{}, {}], 'loads': [{}, {}], 'repeats': 1, 'serviceChecks': True}
        locations = MOD.gate_locations(p)
        self.assertEqual(len(locations), 8)
        self.assertEqual(len({x[0] for x in locations}), 8)
        self.assertIn(('postcheck/p0002.json.identity', 'p0002'), locations)
        self.assertIn(('runs/r0004.json.identity', 'r0004'), locations)
        p['repeats'] = 2000
        with self.assertRaises(MOD.GoRuntimeError):
            MOD.gate_locations(p)

    def test_poll_never_releases_a_changed_process_or_repeats_an_observation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            profile = {'cases': [{}], 'loads': [{}], 'repeats': 1, 'serviceChecks': False,
                       'executor': {'binary': executor()['binary']}}
            watcher = MOD.ProcessObserver(profile, 'c' * 64, root / 'raw', root / 'host', lambda _: '', lambda: None)
            gate = root / 'raw/runs/r0001.json.identity'
            gate.mkdir(parents=True)
            marker = {'id': 'r0001', 'stage': 'ready', 'clock': {'pid': 2, 'process': {
                'startTicks': '123', 'pidNamespace': 'pid:[100]'}}}
            (gate / 'ready.json').write_text(json.dumps(marker))
            def observe(marker, release, output, *args):
                Path(release).write_text('fixture release')
                return {'processBefore': {'hostPid': 2000, 'startTicks': '123'}}
            with patch.object(watcher.observer, 'observe', side_effect=observe) as action:
                watcher.poll()
                watcher.poll()
                self.assertEqual(action.call_count, 1)
                with self.assertRaisesRegex(MOD.GoRuntimeError, 'incomplete'):
                    watcher.finish()
                marker['stage'] = 'finished'
                marker['clock']['process']['startTicks'] = '124'
                (gate / 'finished.json').write_text(json.dumps(marker))
                with self.assertRaisesRegex(MOD.GoRuntimeError, 'changed between gates'):
                    watcher.poll()
                self.assertEqual(action.call_count, 1)
                self.assertFalse((gate / 'finished-release.json').exists())
                marker['clock']['process']['startTicks'] = '123'
                (gate / 'finished.json').write_text(json.dumps(marker))
                watcher.poll()
                result = watcher.finish()
                self.assertEqual(result['gateCount'], 2)
                self.assertEqual(result['processCount'], 1)


if __name__ == '__main__':
    unittest.main()
