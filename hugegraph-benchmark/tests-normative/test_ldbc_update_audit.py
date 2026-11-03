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
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('audit', ROOT / 'tools/ldbc-update-audit.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def result(rows):
    scanner = audit.StreamAudit()
    for row in rows:
        scanner.row(row)
    return scanner.finish()


def comparison(left, right):
    return audit.compare({'streams': {'updateStream_0_0_forum.csv': result(left)}, 'properties': {}},
                         {'streams': {'updateStream_0_0_forum.csv': result(right)}, 'properties': {}})


class UpdateAuditTest(unittest.TestCase):
    def test_classifies_equal_time_order_without_declaring_semantic_interchangeability(self):
        left = [b'100|90|2|1|2|100\n', b'100|91|3|1|3|100\n', b'110|90|5|4|1|110\n']
        right = [left[1], left[0], left[2]]
        r = comparison(left, right)
        stream = r['streams']['updateStream_0_0_forum.csv']
        self.assertEqual('only-order-within-equal-scheduled-time-groups-differs', stream['classification'])
        self.assertFalse(stream['equal']['rawSha256'])
        self.assertTrue(stream['equal']['equalTimeGroupedRawSha256'])
        self.assertFalse(r['databaseReplayValidated'])
        self.assertFalse(r['initialGraphCompatibilityAccepted'])

    def test_dependency_and_timestamp_changes_stay_distinct_from_payload_changes(self):
        a = [b'100|90|2|1|2|100\n']
        checks = [([b'100|91|2|1|2|100\n'], 'same-operation-payloads-per-scheduled-time-dependencies-or-tie-order-differ'),
                  ([b'101|90|2|1|2|100\n'], 'same-operation-payload-order-timestamps-or-dependencies-differ'),
                  ([b'100|90|2|1|3|100\n'], 'operation-content-or-cross-time-order-differs'),
                  (a, 'byte-identical')]
        for b, expected in checks:
            self.assertEqual(expected, comparison(a, b)['streams']['updateStream_0_0_forum.csv']['classification'])

    def test_duplicates_and_nonmonotonic_time_are_not_normalized_away(self):
        a, b = b'100|90|2|1|2|100\n', b'101|90|2|1|3|100\n'
        r = comparison([a, a, b], [a, b, b])['streams']['updateStream_0_0_forum.csv']
        self.assertFalse(r['equal']['equalTimeGroupedRawSha256'])
        r = comparison([a, b], [b, a])['streams']['updateStream_0_0_forum.csv']
        self.assertFalse(r['bothMonotonic'])
        self.assertEqual(1, r['standalone']['outOfOrderRows'])
        self.assertEqual(1, result([b'100|200|2|1|2|100\n'])['futureDependencyRows'])

    def test_signed_driver_metadata_and_explicit_memory_line_limits(self):
        for line in [b'100|90|9|value\n', b'100|-x|1|value\n', b'9223372036854775808|90|1|x\n', b'100|90|1|x']:
            with self.assertRaises(ValueError):
                result([line])
        self.assertEqual(-1, result([b'-1|-2|1|x\n'])['firstScheduled'])
        with self.assertRaisesRegex(ValueError, 'line budget'):
            audit.StreamAudit(max_line_bytes=4).row(b'100|90|1|x\n')
        limited = audit.StreamAudit(max_group_bytes=15)
        limited.row(b'1|0|1|x\n')
        with self.assertRaisesRegex(ValueError, 'memory budget'):
            limited.row(b'1|0|1|x\n')

    def test_partition_and_inventory_drift_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'partition'):
            audit.compare({'streams': {'a': {}}, 'properties': {}}, {'streams': {}, 'properties': {}})
        with self.assertRaisesRegex(ValueError, 'bound'):
            audit.validate_inventory({'id': 'x'}, {'asset': {'id': 'y'}})

    def test_complete_archive_pass_verifies_every_member_and_compressed_bytes(self):
        self.assertIsNotNone(shutil.which('zstd'), 'zstd is required for the archive integration test')
        with tempfile.TemporaryDirectory(prefix='hgbench-update-audit-') as folder:
            root = Path(folder)
            raw = io.BytesIO()
            members = {'data/static/place_0_0.csv': b'id|name\n1|p\n',
                       'data/updateStream_0_0_forum.csv': b'100|90|2|1|2|100\n100|90|3|1|4|100\n',
                       'data/updateStream.properties': b'ldbc.snb.interactive.num_events=2\n'}
            files = []
            with tarfile.open(fileobj=raw, mode='w') as archive:
                for name, value in members.items():
                    entry = tarfile.TarInfo(name)
                    entry.size = len(value)
                    archive.addfile(entry, io.BytesIO(value))
                    meta = {'path': name, 'bytes': len(value), 'sha256': hashlib.sha256(value).hexdigest()}
                    if name.endswith('forum.csv'):
                        meta['rows'] = 2
                    files.append(meta)
            compressed = subprocess.check_output(['zstd', '-q', '-c'], input=raw.getvalue())
            asset = {'id': 'unit', 'role': 'initial', 'scale': '100', 'bytes': len(compressed),
                     'sha256': hashlib.sha256(compressed).hexdigest()}
            (root / (asset['sha256'] + '.tar.zst')).write_bytes(compressed)
            inventory = {'asset': asset, 'files': files,
                         'archiveVerification': 'same-stream-sha256-and-length-before-publication'}
            found = audit.inspect_asset(asset, inventory, root, root, audit.Budget(root, 30, 0), 1024)
            self.assertTrue(found['archiveIntegrityVerified'])
            self.assertEqual(3, found['allMembersVerified'])
            self.assertEqual(2, found['streams']['updateStream_0_0_forum.csv']['rows'])
            wrong = json.loads(json.dumps(inventory))
            wrong['files'][0]['sha256'] = '0' * 64
            with self.assertRaisesRegex(ValueError, 'identity drift'):
                audit.inspect_asset(asset, wrong, root, root, audit.Budget(root, 30, 0), 1024)


if __name__ == '__main__':
    unittest.main()
