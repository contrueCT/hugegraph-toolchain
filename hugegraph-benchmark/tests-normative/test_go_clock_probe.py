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
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "components/http/go-clock-probe.py"
SPEC = importlib.util.spec_from_file_location("hgbench_go_clock_probe", MODULE_PATH)
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


class GoClockProbeCleanup(unittest.TestCase):
    def exercise(self, foreign=False):
        with tempfile.TemporaryDirectory(prefix="hgbench-clock-cleanup-") as folder:
            root = Path(folder)
            binary = root / "runner"
            binary.write_bytes(b"fixture")
            binary.chmod(0o700)
            calls = []
            owner = None
            removed = False

            def command(args, **kwargs):
                nonlocal owner, removed
                calls.append(args)
                if args[1] == "create":
                    owner = args[args.index("--label") + 1].split("=", 1)[1]
                    raise subprocess.TimeoutExpired(args, 20)
                if args[1] == "inspect":
                    if removed:
                        return subprocess.CompletedProcess(args, 1, b"", b"missing")
                    info = {"Id": "c" * 64, "Config": {"Labels": {
                        "org.apache.hugegraph.hgbench.goClock": "foreign" if foreign else owner}}}
                    return subprocess.CompletedProcess(args, 0, json.dumps([info]).encode(), b"")
                if args[1] == "rm":
                    self.assertEqual(args[-1], "c" * 64)
                    removed = True
                    return subprocess.CompletedProcess(args, 0, ("c" * 64).encode(), b"")
                self.fail("unexpected Docker action: " + str(args))

            with patch.object(PROBE.sys, "platform", "linux"), patch.object(PROBE, "process_identity", return_value={}), \
                    patch.object(PROBE.subprocess, "run", side_effect=command), contextlib.redirect_stdout(io.StringIO()):
                status = PROBE.probe(binary, "sha256:" + "a" * 64, root / "output")
            proof = json.loads((root / "output/proof.json").read_text())
            self.assertEqual(status, 1)
            self.assertTrue(proof["error"].startswith("TimeoutExpired:"))
            self.assertEqual(sum(args[1] == "create" for args in calls), 1)
            self.assertEqual(calls[1][1:2], ["inspect"])
            self.assertTrue(calls[1][-1].startswith("hgbench-go-clock-"))
            self.assertEqual(removed, not foreign)
            if foreign:
                self.assertFalse(proof["cleanup"]["removed"])
                self.assertIn("ownership mismatch", proof["cleanup"]["error"])
            else:
                self.assertEqual(proof["cleanup"], {"removed": True, "absent": True})
            lock = json.loads((root / "output/raw.lock.json").read_text())
            self.assertEqual({row["path"] for row in lock["files"]}, {"producer.py", "proof.json"})

    def test_uncertain_create_is_looked_up_and_cleaned_without_restart(self):
        self.exercise()

    def test_foreign_owner_is_never_removed(self):
        self.exercise(foreign=True)


if __name__ == "__main__":
    unittest.main()
