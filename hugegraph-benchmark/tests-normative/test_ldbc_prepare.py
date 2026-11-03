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
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parent.parent / "tools/ldbc-prepare.py"
spec = importlib.util.spec_from_file_location("ldbc_prepare", MODULE)
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PreparationTests(unittest.TestCase):
    def test_signed_64_bit_ids_and_embedded_json_delimiters_are_lossless(self):
        self.assertEqual(prepare.identity("post", "9223372036854775807"), "message:9223372036854775807")
        self.assertEqual(prepare.typed("ldbcId", "9007199254740993"), 9007199254740993)
        with self.assertRaises(ValueError):
            prepare.identity("person", "9223372036854775808")
        operation, result = prepare.parse_validation('{"content":"a|b"}|{"messageContent":"c|d"}\n')
        self.assertEqual(operation["content"], "a|b")
        self.assertEqual(result["messageContent"], "c|d")

    def test_all_entity_types_parallel_edges_and_multi_properties_are_preserved(self):
        identifier = "9007199254740993"
        tables = {
            "person": f"id|firstName|lastName|birthday|creationDate|locationIP|browserUsed|gender\n{identifier}|Ada|L|1|2|127.0.0.1|Browser|female\n",
            "post": "id|creationDate|content|imageFile|length\n42|3|hello||5\n",
            "comment": "id|creationDate|content|length\n43|4|reply|5\n",
            "place": "id|name|url|type\n7|City|url|city\n",
            "forum": "id|title|creationDate\n8|Forum|1\n",
            "organisation": "id|name|url|type\n9|Org|url|company\n",
            "tag": "id|name|url\n10|Tag|url\n",
            "tagclass": "id|name|url\n11|Class|url\n",
            "person_email_emailaddress": f"Person.id|email\n{identifier}|a@example.test\n{identifier}|b@example.test\n",
            "person_speaks_language": f"Person.id|language\n{identifier}|en\n",
            "person_isLocatedIn_place": f"Person.id|Place.id\n{identifier}|7\n",
            "post_hasCreator_person": f"Post.id|Person.id\n42|{identifier}\n",
            "comment_hasCreator_person": f"Comment.id|Person.id\n43|{identifier}\n",
            "person_likes_post": f"Person.id|Post.id|creationDate\n{identifier}|42|5\n{identifier}|42|6\n",
        }
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            initial = base / "initial"
            (initial / "dynamic").mkdir(parents=True)
            asset = {"id": "fixture", "role": "initial", "scale": "0.1"}
            files = []
            for name, body in tables.items():
                path = initial / "dynamic" / (name + "_0_0.csv")
                path.write_text(body)
                digest, size = prepare.assets.digest_file(path)
                files.append({"path": str(path.relative_to(initial)), "bytes": size, "sha256": digest})
            (initial / "extraction-manifest.json").write_text(json.dumps({"asset": asset, "files": files}))
            frozen = {"serializer": "CsvBasic", "dateFormatter": "LongDateFormatter", "assets": [asset]}
            result = prepare.prepare(initial, base / "out", frozen)
            self.assertEqual(result["vertices"], 8)
            self.assertEqual(result["edges"], 5)
            self.assertEqual(result["counts"]["person_email_emailaddress"], 2)
            person = json.loads((base / "out/vertices/person.json").read_text())
            self.assertEqual(person["ldbcId"], int(identifier))
            self.assertEqual(person["emails"], ["a@example.test", "b@example.test"])
            post = json.loads((base / "out/vertices/post.json").read_text())
            self.assertNotIn("imageFile", post)
            likes = [json.loads(line) for line in (base / "out/edges/person_likes_post.json").read_text().splitlines()]
            self.assertEqual([edge["edgeKey"] for edge in likes], [1, 2])
            self.assertEqual(likes[0]["sourceId"], "person:" + identifier)
            with sqlite3.connect(base / "out/reference.sqlite") as db:
                self.assertEqual(db.execute("SELECT count(*) FROM vertices").fetchone()[0], 8)
            self.assertIn('sortKeys("edgeKey")', (base / "out/schema.groovy").read_text())
            limits = {"shard_rows": 1, "shard_bytes": 4096, "max_data_bytes": 1024 ** 2,
                      "max_index_bytes": 4 * 1024 ** 2}
            sharded = base / "sharded"
            shard_result = prepare.prepare(initial, sharded, frozen, **limits)
            self.assertEqual(shard_result["vertices"], result["vertices"])
            self.assertEqual(shard_result["edges"], result["edges"])
            for category in ("vertices", "edges"):
                for original in (base / "out" / category).glob("*.json"):
                    actual = "".join((sharded / shard["path"]).read_text() for shard in shard_result["shards"]
                                     if shard["category"] == category and shard["label"] == original.stem)
                    self.assertEqual(actual, original.read_text())
            self.assertTrue(all(s["records"] == "1" for s in shard_result["shards"]))
            with sqlite3.connect(sharded / "reference.sqlite") as db:
                self.assertEqual(db.execute("SELECT count(*) FROM edges").fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT count(*) FROM vertices WHERE body != ''").fetchone()[0], 0)
            self.assertEqual(prepare.prepare(initial, sharded, frozen, resume=True, **limits), shard_result)
            with self.assertRaisesRegex(ValueError, "identity changed"):
                prepare.prepare(initial, sharded, frozen, resume=True, **{**limits, "shard_rows": 2})
            interrupted = base / "interrupted"
            original_write = prepare.shards.ShardWriter.write
            calls = 0
            def stop(writer, line):
                nonlocal calls
                calls += 1
                if calls == 12:
                    raise KeyboardInterrupt()
                return original_write(writer, line)
            with patch.object(prepare.shards.ShardWriter, "write", stop):
                with self.assertRaises(KeyboardInterrupt):
                    prepare.prepare(initial, interrupted, frozen, **limits)
            self.assertFalse((interrupted / "prepare-manifest.json").exists())
            self.assertFalse((interrupted / ".conversion.lock").exists())
            resumed = prepare.prepare(initial, interrupted, frozen, resume=True, **limits)
            self.assertGreaterEqual(resumed["reusedShards"], 8)
            self.assertEqual(resumed["shards"], shard_result["shards"])
            zipped = base / "compressed"
            gzip_limits = {**limits, "compression": "gzip", "max_stored_bytes": 1024 ** 2}
            zipped_result = prepare.prepare(initial, zipped, frozen, **gzip_limits)
            self.assertEqual(zipped_result["schemaVersion"], 2)
            self.assertEqual(shard_result["schemaVersion"], 1)
            self.assertEqual(zipped_result["counts"], shard_result["counts"])
            zipped_struct = json.loads((zipped / "struct.json").read_text())
            for category in ("vertices", "edges"):
                for entry in zipped_struct[category]:
                    self.assertEqual(entry["input"]["compression"], "GZIP")
                for original in (base / "out" / category).glob("*.json"):
                    decoded = b"".join(gzip.decompress((zipped / shard["path"]).read_bytes())
                        for shard in zipped_result["shards"] if shard["category"] == category and shard["label"] == original.stem)
                    self.assertEqual(decoded, original.read_bytes())
            self.assertEqual(prepare.prepare(initial, zipped, frozen, resume=True, **gzip_limits), zipped_result)
            for change in ({"compression": "none"}, {"max_stored_bytes": 2048}):
                with self.assertRaisesRegex(ValueError, "identity changed"):
                    prepare.prepare(initial, zipped, frozen, resume=True, **{**gzip_limits, **change})
            gzip_interrupted = base / "gzip-interrupted"
            calls = 0
            with patch.object(prepare.shards.ShardWriter, "write", stop):
                with self.assertRaises(KeyboardInterrupt):
                    prepare.prepare(initial, gzip_interrupted, frozen, **gzip_limits)
            gzip_resumed = prepare.prepare(initial, gzip_interrupted, frozen, resume=True, **gzip_limits)
            self.assertGreaterEqual(gzip_resumed["reusedShards"], 8)
            self.assertEqual(gzip_resumed["shards"], zipped_result["shards"])
            with self.assertRaisesRegex(ValueError, "byte budget"):
                prepare.prepare(initial, base / "too-small", frozen, **{**limits, "max_data_bytes": 1})
            (sharded / shard_result["shards"][0]["path"]).write_text("tampered")
            with self.assertRaisesRegex(ValueError, "output drift"):
                prepare.prepare(initial, sharded, frozen, resume=True, **limits)
            # Post and Comment share ID encoding; endpoint labels must also match.
            wrong = initial / "dynamic/person_likes_post_0_0.csv"
            wrong.write_text(f"Person.id|Post.id|creationDate\n{identifier}|43|5\n")
            for entry in files:
                if entry["path"] == str(wrong.relative_to(initial)):
                    entry["sha256"], entry["bytes"] = prepare.assets.digest_file(wrong)
            (initial / "extraction-manifest.json").write_text(json.dumps({"asset": asset, "files": files}))
            with self.assertRaisesRegex(ValueError, "endpoints"):
                prepare.prepare(initial, base / "wrong-label", frozen, **limits)
            (initial / "dynamic/post_0_0.csv").write_text("corrupt")
            with self.assertRaisesRegex(ValueError, "drift"):
                prepare.prepare(initial, base / "should-not-exist", frozen)
            self.assertFalse((base / "should-not-exist").exists())


if __name__ == "__main__":
    unittest.main()
