# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file under the Apache License, Version 2.0 (the "License"); you may not use this
# file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
import importlib.util
from pathlib import Path
import subprocess
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("clock_correlation", ROOT / "components/http/clock-correlation.py")
CLOCK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLOCK)


class ClockTransportTest(unittest.TestCase):
    def run_peer(self, code):
        child = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        transport = CLOCK.Transport(child)

        def cleanup():
            CLOCK.terminate(child)
            transport.close()
            for stream in (child.stdin, child.stdout, child.stderr):
                stream.close()

        self.addCleanup(cleanup)
        return child, transport

    def test_dripping_response_has_absolute_deadline_and_child_reaped(self):
        child, transport = self.run_peer(
            "import sys,time\nsys.stdin.readline()\nwhile True:\n sys.stdout.write('x');sys.stdout.flush();time.sleep(.01)\n")
        begin = time.monotonic_ns()
        with self.assertRaisesRegex(ValueError, "WALL_DEADLINE"):
            transport.exchange(b"{}\n", begin + 150000000)
        self.assertLess(time.monotonic_ns() - begin, 1000000000)
        CLOCK.terminate(child)
        self.assertIsNotNone(child.poll())
        self.assertLess(len(transport.output), 16384)

    def test_unterminated_large_frame_rejected_during_read(self):
        _, transport = self.run_peer("import sys\nsys.stdin.readline()\nsys.stdout.write('x'*30000);sys.stdout.flush()\n")
        with self.assertRaisesRegex(ValueError, "RESPONSE_BUDGET"):
            transport.exchange(b"{}\n", time.monotonic_ns() + 2000000000)

    def test_stderr_budget_is_independent_of_stdout(self):
        _, transport = self.run_peer("import sys\nsys.stdin.readline()\nsys.stderr.write('x'*30000);sys.stderr.flush()\n")
        with self.assertRaisesRegex(ValueError, "STDERR_BUDGET"):
            transport.exchange(b"{}\n", time.monotonic_ns() + 2000000000)

    def test_duplicate_response_does_not_become_next_nonce(self):
        _, transport = self.run_peer("import sys\nsys.stdin.readline()\nsys.stdout.write('{}\\n{}\\n');sys.stdout.flush()\n")
        with self.assertRaisesRegex(ValueError, "UNSOLICITED_OUTPUT"):
            transport.exchange(b"{}\n", time.monotonic_ns() + 2000000000)

    def test_eof_is_not_successful_anchor(self):
        _, transport = self.run_peer("import sys\nsys.stdin.readline()\n")
        with self.assertRaisesRegex(ValueError, "PEER_EOF"):
            transport.exchange(b"{}\n", time.monotonic_ns() + 2000000000)

    def test_cancellation_observed_before_exchange(self):
        _, transport = self.run_peer("import time\ntime.sleep(5)\n")
        CLOCK.CANCELLED = True
        self.addCleanup(setattr, CLOCK, "CANCELLED", False)
        with self.assertRaisesRegex(ValueError, "CANCELLED"):
            transport.exchange(b"{}\n", time.monotonic_ns() + 2000000000)


if __name__ == "__main__":
    unittest.main()
