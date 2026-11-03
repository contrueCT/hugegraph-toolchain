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
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("go_process_observer", HERE / "components/http/go-process-observer.py")
OBSERVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OBSERVER)


class LiveProcessObservation(unittest.TestCase):
    def exercise(self, wrong_executable=False):
        binary = {"sha256": "a" * 64, "sourceSha256": "b" * 64, "goVersion": "go1.27.3", "goarch": "amd64"}
        proc = {"bootId": "boot", "startTicks": "100", "timeOffsets": "monotonic 0 0\nboottime 0 0"}
        for ns in ("pid", "mnt", "time", "time_for_children", "cgroup"):
            proc[ns + "Namespace"] = ns + ":[123]"
        observed = {**proc, "hostPid": 2000, "namespacePids": [2000, 2],
                    "executableSha256": "f" * 64 if wrong_executable else binary["sha256"]}
        marker = {"schemaVersion": 1, "kind": "http-go-live-process-marker", "stage": "ready", "id": "r0001",
                  "nonce": "c" * 32, "binary": {"binarySha256": binary["sha256"], **binary, "goos": "linux"},
                  "clock": {"pid": 2, "process": proc}}
        cid = "d" * 64
        calls = []

        def docker(args):
            calls.append(args)
            if args[0] == "inspect":
                return json.dumps([{"Id": cid, "Image": "sha256:" + "e" * 64,
                                    "State": {"Running": True, "Pid": 1000}}])
            self.assertEqual(args, ["top", cid, "-eo", "pid"])
            return "PID\n2000\n"

        with tempfile.TemporaryDirectory(prefix="hgbench-live-process-") as folder:
            root = Path(folder)
            raw = (json.dumps(marker) + "\n").encode()
            (root / "ready.json").write_bytes(raw)
            with patch.object(OBSERVER.os, "readlink", return_value=proc["pidNamespace"]), \
                    patch.object(OBSERVER, "proc_status", return_value="NSpid:\t2000\t2\n"), \
                    patch.object(OBSERVER.CLOCK, "process_identity", side_effect=lambda pid: proc if pid == os.getpid() else observed):
                run = lambda: OBSERVER.observe(root / "ready.json", root / "release.json", root / "receipt.json",
                                               binary, cid, docker)
                if wrong_executable:
                    with self.assertRaisesRegex(ValueError, "actual executable SHA"):
                        run()
                else:
                    result = run()
                    self.assertIsNone(result["error"])
            receipt = json.loads((root / "receipt.json").read_text())
            self.assertEqual(len(calls), 2)
            self.assertEqual((root / "release.json").exists(), not wrong_executable)
            if wrong_executable:
                self.assertIsNotNone(receipt["error"])
            else:
                ack = json.loads((root / "release.json").read_text())
                self.assertEqual(ack["markerSha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(ack["nonce"], marker["nonce"])
                self.assertEqual(receipt["releaseRaw"], (root / "release.json").read_text())
                self.assertEqual(receipt["processBefore"], receipt["processAfter"])
                self.assertLessEqual(int(receipt["hostBeforeNs"]), int(receipt["hostAfterNs"]))

    def test_exact_held_pid_is_recorded_before_release(self):
        self.exercise()

    def test_executable_mismatch_never_releases_http(self):
        self.exercise(wrong_executable=True)


if __name__ == "__main__":
    unittest.main()
