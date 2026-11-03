# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Peer protocol fault fixtures. Synthetic replies are never actual-Node or performance evidence."""
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
PEER = ROOT / 'components/http/phase-clock-reference.py'
SPEC = dict(schemaVersion=1, kind='http-runner-sanity-spec', sourceOrigin='fixture', startupMs=2000,
            exchangeMs=1000, finishMs=500, wallMs=10000, maxFrameBytes=8192, maxRawBytes=65536,
            maxStderrBytes=2048)
BINDING = dict(sessionId='a' * 32, runIdentity=dict(id='r1', caseId='c1', loadId='l1', repeat=0,
                                                probePhase=None), profileDigest='b' * 64)


class Peer(unittest.TestCase):
    def start(self, path=PEER, spec=None):
        child = subprocess.Popen([sys.executable, '-I', '-S', '-B', str(path), '--spec-json',
                                  json.dumps(spec or SPEC)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        def close():
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=3)
        self.addCleanup(close)
        self.send(child, dict(kind='init', binding=BINDING))
        ready = self.line(child)
        self.assertEqual(ready['kind'], 'ready')
        self.assertEqual(ready['identity']['pid'], child.pid)
        self.assertEqual(ready['identity']['parentPid'], os.getpid())
        self.assertEqual(ready['identity']['sourceOrigin'], 'fixture')
        return child

    def send(self, child, value):
        child.stdin.write((json.dumps(value) + '\n').encode())
        child.stdin.flush()

    def line(self, child):
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(3), 'reference frame deadline expired')
        return json.loads(child.stdout.readline())

    def exchange(self, child, stage, wrong_nonce=False):
        self.send(child, dict(kind='sample', binding=BINDING, stage=stage))
        challenge = self.line(child)
        self.assertEqual(challenge['kind'], 'challenge')
        self.send(child, dict(kind='anchor', binding=BINDING, stage=stage, sequence=challenge['sequence'],
                             nonce='0' * 32 if wrong_nonce else challenge['nonce'], anchor={'fixture': True}))
        return None if wrong_nonce else self.line(child)

    def failed(self, child, expected):
        stdout, stderr = child.communicate(timeout=3)
        self.assertEqual(child.returncode, 6, stdout)
        self.assertIn(expected, stderr.decode())
        self.assertLessEqual(len(stderr), SPEC['maxStderrBytes'])

    def test_wrong_nonce_is_rejected_by_the_actual_python_receive_path(self):
        child = self.start()
        self.exchange(child, 'pre', wrong_nonce=True)
        self.failed(child, 'SR_RESPONSE_BINDING')

    def test_replayed_or_out_of_order_stage_is_rejected(self):
        child = self.start()
        self.send(child, dict(kind='sample', binding=BINDING, stage='post'))
        self.failed(child, 'SR_STAGE')

    def test_peer_wall_deadline_applies_while_idle_without_sampling(self):
        spec = dict(SPEC, startupMs=500, exchangeMs=100, finishMs=100, wallMs=1000)
        started = time.monotonic()
        child = self.start(spec=spec)
        child.wait(timeout=3)
        self.failed(child, 'SR_DEADLINE')
        self.assertLess(time.monotonic() - started, 3)

    def test_changed_reference_source_fails_final_metadata_recheck(self):
        with tempfile.TemporaryDirectory(prefix='hgbench-sr-reference-copy-') as temporary:
            peer = Path(temporary) / PEER.name
            shutil.copyfile(PEER, peer)
            child = self.start(path=peer)
            for stage in ('pre', 'post'):
                receipt = self.exchange(child, stage)
                self.assertEqual(receipt['kind'], 'receipt')
                self.assertLessEqual(int(receipt['sentMonotonicNs']), int(receipt['receivedMonotonicNs']))
            with peer.open('a') as source:
                source.write('\n# changed after raw exchanges\n')
            self.send(child, dict(kind='finish', binding=BINDING))
            self.failed(child, 'SR_REFERENCE_FILE_DRIFT')


if __name__ == '__main__':
    unittest.main()
