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

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('loader_text', ROOT / 'tools/build-ldbc-loader-text.py')
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)


class LoaderTextBuildTests(unittest.TestCase):
    def fixture(self, root, duplicate=False):
        source = root / 'DataTypeUtil.java'
        source.write_text(b.ORIGINAL + '\n            value = ((String) rawValue).trim();\n        }\n')
        loader = root / 'loader'
        (loader / 'lib').mkdir(parents=True)
        for name in ['native.jar'] + (['ambiguous-overlay.jar'] if duplicate else []):
            with zipfile.ZipFile(loader / 'lib' / name, 'x') as jar:
                jar.writestr(b.CLASS, name.encode())
        jdk = root / 'jdk'
        (jdk / 'bin').mkdir(parents=True)
        for name in ['java', 'javac']:
            (jdk / 'bin' / name).write_text('unused')
        return source, b.sha(source), loader, jdk, root / 'output'

    def test_source_identity_and_unique_supported_block_precede_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self.fixture(Path(temp))
            original = args[0].read_bytes()
            with patch.object(b, 'bounded', side_effect=AssertionError('Must not run Java')):
                with self.assertRaisesRegex(ValueError, 'reviewed SHA-256'):
                    b.create(args[0], '0' * 64, *args[2:])
            self.assertEqual(args[0].read_bytes(), original)
            self.assertFalse(args[-1].exists())
            for raw in [b'unknown source', (b.ORIGINAL * 2).encode()]:
                with self.assertRaisesRegex(ValueError, 'unique'):
                    b.transform(raw, hashlib.sha256(raw).hexdigest())

    def test_ambiguous_classpath_is_rejected_without_building_or_editing_jars(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self.fixture(Path(temp), duplicate=True)
            before = {path.name: b.sha(path) for path in (args[2] / 'lib').iterdir()}
            with patch.object(b, 'bounded', side_effect=AssertionError('Must not run Java')):
                with self.assertRaisesRegex(ValueError, 'Conflicting native'):
                    b.create(*args)
            self.assertEqual(before, {path.name: b.sha(path) for path in (args[2] / 'lib').iterdir()})
            self.assertFalse(args[-1].exists())

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self.fixture(Path(temp))
            args[-1].mkdir()
            marker = args[-1] / 'keep'
            marker.write_bytes(b'existing evidence')
            with patch.object(b, 'bounded', side_effect=AssertionError('Must not run Java')):
                with self.assertRaises(FileExistsError):
                    b.create(*args)
            self.assertEqual(marker.read_bytes(), b'existing evidence')
            self.assertEqual(sorted(path.name for path in args[-1].iterdir()), ['keep'])

    def test_zero_exit_without_compiled_artifact_has_no_success_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self.fixture(Path(temp))
            with patch.object(b, 'bounded', return_value=None):
                with self.assertRaisesRegex(ValueError, 'Missing compiled'):
                    b.create(*args)
            self.assertFalse((args[-1] / 'build-manifest.json').exists())
            failure = json.loads((args[-1] / 'failure.json').read_text())
            self.assertEqual(failure['status'], 'failed')
            self.assertFalse(failure['databaseExecuted'])
            self.assertFalse(any(path.name.startswith('compile-') for path in args[-1].iterdir()))


if __name__ == '__main__':
    unittest.main()
