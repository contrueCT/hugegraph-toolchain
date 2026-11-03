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
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1] / "benchmarks/http-serving-v0.1"


class HttpAnswers(unittest.TestCase):
    def test_independent_python_answers(self):
        answers = {a["id"]: a for a in json.loads((ROOT / "answers.json").read_text())["cases"]}
        # Direct oracle formulas, no import/execution of the JS/Groovy implementation.
        vectors = {"scalar-add": [42], "structured-one": [dict(id=7, name="hgbench", values=[1, 2, 3], active=True)]}
        for n in (16, 256, 4096):
            vectors[f"rows-{n:04d}"] = [dict(id=i, name="hgbench", value=3*i, active=i % 2 == 0) for i in range(n)]
        for kib in (1, 16, 64, 256, 1024):
            vectors[f"string-{kib:04d}k"] = ["x" * (kib*1024)]
        text = "汉图🙂" + chr(34) + chr(92) + chr(10)
        vectors["string-unicode"] = [text * 512]
        self.assertEqual(set(vectors), set(answers))
        for key, data in vectors.items():
            with self.subTest(case=key):
                # Object keys are ASCII in this suite, so Python/JS ordering coincides.
                raw = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.assertEqual(hashlib.sha256(raw).hexdigest(), answers[key]["canonicalSha256"])
                self.assertEqual(len(raw), answers[key]["resultDataCanonicalBytes"])
                self.assertEqual(len(data), answers[key]["resultRows"])
                if isinstance(data[0], str):
                    self.assertEqual(len(data[0].encode("utf-8")), answers[key]["payloadUtf8Bytes"])


if __name__ == "__main__":
    unittest.main()
