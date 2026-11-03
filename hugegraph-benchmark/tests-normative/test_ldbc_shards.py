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

import gzip
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("shards", Path(__file__).resolve().parents[1] / "tools/ldbc-shards.py")
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


class CompressedShardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.line = '{"vertexId":"person:9007199254740993","name":"甲","ldbcId":9007199254740993}\n'

    def writer(self, name, **kwargs):
        root = self.root / name
        (root / "vertices").mkdir(parents=True, exist_ok=True)
        options = dict(rows=2, shard_bytes=4096, data_bytes=4096, compression="gzip", stored_bytes=4096)
        options.update(kwargs)
        owner = s.ShardSet(root, **options)
        return owner, owner.writer("vertices", "person")

    def test_fixed_headers_exact_content_and_deterministic_replay(self):
        manifests = []
        for folder in ("a", "b", "a"):
            owner, writer = self.writer(folder)
            for _ in range(3):
                writer.write(self.line)
            writer.close()
            self.assertEqual(owner.reused, 2 if len(manifests) == 2 else 0)
            self.assertEqual(owner.total, len(self.line.encode()) * 3)
            self.assertEqual(owner.stored, sum(int(f["bytes"]) for f in owner.files))
            for item in owner.files:
                raw = (owner.output / item["path"]).read_bytes()
                self.assertEqual(raw[4:8], b"\0" * 4)
                self.assertEqual(raw[3] & 8, 0)  # no filename in gzip header
                decoded = gzip.decompress(raw)
                self.assertEqual(decoded, self.line.encode() * int(item["records"]))
                self.assertEqual(item["encoding"]["uncompressedSha256"], s.hashlib.sha256(decoded).hexdigest())
                self.assertEqual(int(item["encoding"]["uncompressedBytes"]), len(decoded))
                self.assertEqual(s.digest(owner.output / item["path"]), (item["sha256"], int(item["bytes"])))
            manifests.append(owner.files)
        self.assertEqual(manifests[0], manifests[1])
        self.assertEqual(manifests[0], manifests[2])

    def test_logical_and_stored_budgets_include_compression_trailer(self):
        owner, writer = self.writer("baseline")
        writer.write(self.line)
        writer.close()
        exact_size = int(owner.files[0]["bytes"])
        for name, limits in (("logical", {"data_bytes": 1}), ("header", {"stored_bytes": 1}),
                             ("trailer", {"stored_bytes": exact_size - 1})):
            owner, writer = self.writer(name, **limits)
            try:
                with self.assertRaisesRegex(ValueError, "byte budget"):
                    writer.write(self.line)
                    writer.close()
            finally:
                writer.abort()
            self.assertEqual(list((owner.output / "vertices").iterdir()), [])
            self.assertLessEqual(owner.stored, owner.stored_bytes)
        owner, writer = self.writer("exact", stored_bytes=exact_size)
        writer.write(self.line)
        writer.close()
        self.assertEqual(owner.stored, exact_size)

    def test_replay_rejects_corrupted_compressed_bytes(self):
        owner, writer = self.writer("corrupt")
        writer.write(self.line)
        writer.close()
        path = owner.output / owner.files[0]["path"]
        raw = bytearray(path.read_bytes())
        raw[-8] ^= 1
        path.write_bytes(raw)
        _, writer = self.writer("corrupt")
        writer.write(self.line)
        with self.assertRaisesRegex(ValueError, "deterministic replay"):
            writer.close()

    def test_abort_keeps_sealed_shards_only(self):
        owner, writer = self.writer("interrupted", rows=1)
        writer.write(self.line)
        writer.write(self.line)
        writer.abort()
        self.assertEqual(len(owner.files), 1)
        self.assertEqual(len(list((owner.output / "vertices").iterdir())), 1)
        resumed, writer = self.writer("interrupted", rows=1)
        writer.write(self.line)
        writer.write(self.line)
        writer.close()
        self.assertEqual(resumed.reused, 1)
        self.assertEqual(len(resumed.files), 2)


if __name__ == "__main__":
    unittest.main()
