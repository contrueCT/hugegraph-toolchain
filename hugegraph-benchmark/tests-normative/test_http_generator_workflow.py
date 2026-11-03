# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file under the Apache License, Version 2.0
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
import time
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('generator_workflow', Path(__file__).resolve().parents[1] /
    'components/http/generator-workflow.py')
G = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(G)


class GeneratorSafety(unittest.TestCase):
    def test_cancel_deadline_and_disk_floor_prevent_any_command_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            study = G.Study.__new__(G.Study)
            study.root = study.out = Path(directory)
            study.cancelled = True
            study.end = time.monotonic() + 60
            study.budget = {'cleanupReserveMs': 5000, 'minFreeBytes': 0, 'maxEvidenceBytes': 16777216}
            study.last_scan = float('-inf')
            with patch.object(G.subprocess, 'run') as command:
                with self.assertRaisesRegex(ValueError, 'cancelled'):
                    study.command(['must-not-run'])
                study.cancelled = False
                study.end = time.monotonic() + 4
                with self.assertRaisesRegex(ValueError, 'task-hard-deadline'):
                    study.command(['must-not-run'])
                study.end = time.monotonic() + 60
                study.budget['minFreeBytes'] = 2 ** 64
                study.last_scan = float('-inf')
                with self.assertRaisesRegex(ValueError, 'disk-floor'):
                    study.command(['must-not-run'])
                command.assert_not_called()

    def test_inventory_rejects_escape_duplicate_symlink_and_modified_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'good').write_text('locked')
            entry = {'path': 'good', 'bytes': 6, 'sha256': G.sha(root / 'good')}
            G.validate_inventory(root, [entry])
            for entries in [[{**entry, 'path': '../outside'}], [entry, entry], [{**entry, 'sha256': 'a' * 64}]]:
                with self.assertRaises(ValueError):
                    G.validate_inventory(root, entries)
            (root / 'alias').symlink_to(root / 'good')
            with self.assertRaises(ValueError):
                G.validate_inventory(root, [{**entry, 'path': 'alias'}])

    def test_cleanup_refuses_foreign_container_and_never_uses_wildcard_cleanup(self):
        study = G.Study.__new__(G.Study)
        study.owner = 'ours'
        study.inspect = lambda *a, **kw: {'Config': {'Labels': {'hgbench.generator.owner': 'someone-else'}}}
        calls = []
        study.docker = lambda args, **kw: calls.append(args)
        with self.assertRaisesRegex(ValueError, 'cleanup-ownership'):
            study.stop_container('precise-id')
        self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
