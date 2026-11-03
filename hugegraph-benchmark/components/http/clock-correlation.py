#!/usr/bin/env python3
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
"""Bounded local Python-reference / Node-peer causal clock correlation capture."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve()
PEER = HERE.with_suffix(".mjs")
CANCELLED = False


def need(condition, code):
    if not condition:
        raise ValueError(code)


def check(deadline):
    need(not CANCELLED, "CLOCK_CANCELLED")
    need(time.monotonic_ns() < deadline, "CLOCK_WALL_DEADLINE")


def integer(value, low, high):
    return type(value) is int and low <= value <= high


def spec_check(s):
    fields = {"schemaVersion", "kind", "sourceOrigin", "sampleCount", "intervalMs", "exchangeTimeoutMs",
              "wallMs", "maxEvidenceBytes", "maxRttNs", "maxMappingWidthNs", "maxDriftPpm"}
    need(type(s) is dict and set(s) == fields, "CLOCK_SPEC_FIELDS")
    need(s["schemaVersion"] == 1 and s["kind"] == "http-clock-correlation-spec", "CLOCK_SPEC_KIND")
    need(s["sourceOrigin"] in ("fixture", "linux-procfs"), "CLOCK_SOURCE_ORIGIN")
    for key, low, high in [("sampleCount", 2, 1000), ("intervalMs", 1, 60000), ("exchangeTimeoutMs", 100, 10000),
                           ("wallMs", 1000, 120000), ("maxEvidenceBytes", 65536, 16 * 1024 * 1024),
                           ("maxDriftPpm", 0, 100000)]:
        need(integer(s[key], low, high), "CLOCK_SPEC_BUDGET")
    for key in ("maxRttNs", "maxMappingWidthNs"):
        need(type(s[key]) is str and re.fullmatch(r"[1-9][0-9]{0,24}", s[key]), "CLOCK_INTEGER_PRECISION")
    need((s["sampleCount"] - 1) * s["intervalMs"] < s["wallMs"], "CLOCK_SPEC_BUDGET")
    need(int(s["maxRttNs"]) <= s["exchangeTimeoutMs"] * 1000000 and
         int(s["maxMappingWidthNs"]) <= s["wallMs"] * 1000000, "CLOCK_SPEC_BUDGET")
    return s


def digest(path, deadline):
    h = hashlib.sha256()
    check(deadline)
    with open(path, "rb") as stream:
        before = os.fstat(stream.fileno())
        while True:
            check(deadline)
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
        after = os.fstat(stream.fileno())
    need((before.st_size, before.st_mtime_ns, before.st_ino) ==
         (after.st_size, after.st_mtime_ns, after.st_ino), "CLOCK_INPUT_CHANGED")
    check(deadline)
    return {"path": str(path), "bytes": before.st_size, "sha256": h.hexdigest()}


def identity(origin, token, runtime):
    result = dict(sourceOrigin=origin, platform=sys.platform, pid=os.getpid(), processToken=token,
                  bootId=None, startTimeTicks=None, timeNamespace=None, timeForChildrenNamespace=None,
                  timeOffsets=None, runtime=runtime)
    if origin == "linux-procfs":
        need(sys.platform == "linux", "CLOCK_LINUX_REQUIRED")

        def stat():
            raw = Path("/proc/self/stat").read_text()
            tail = raw[raw.rfind(")") + 2:].split()
            need(len(tail) >= 20 and tail[0] not in ("Z", "X", "x"), "CLOCK_PROCESS_STATE")
            need(re.fullmatch(r"[0-9]+", tail[19]), "CLOCK_PROCESS_IDENTITY")
            return tail[19]

        result["startTimeTicks"] = stat()
        result["bootId"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        result["timeNamespace"] = os.readlink("/proc/self/ns/time")
        result["timeForChildrenNamespace"] = os.readlink("/proc/self/ns/time_for_children")
        result["timeOffsets"] = Path("/proc/self/timens_offsets").read_text()
        need(stat() == result["startTimeTicks"], "CLOCK_PROCESS_CHANGED")
    return result


def raw_json(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def save(path, value):
    with open(path, "xb") as stream:
        stream.write(raw_json(value))


class Transport:
    """Absolute deadlines apply while stdout/stderr are drained, including drip feeds."""
    def __init__(self, child):
        self.child = child
        self.selector = selectors.DefaultSelector()
        self.output = bytearray()
        self.stderr = bytearray()
        self.stdout_bytes = 0
        for stream, role in ((child.stdout, "out"), (child.stderr, "err")):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, role)
        os.set_blocking(child.stdin.fileno(), False)

    def pump(self, deadline):
        check(deadline)
        for key, _ in self.selector.select(min(0.1, max(0, (deadline - time.monotonic_ns()) / 1e9))):
            chunk = os.read(key.fileobj.fileno(), 16385)
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            if key.data == "out":
                self.stdout_bytes += len(chunk)
                self.output.extend(chunk)
                need(len(self.output) <= 16384, "CLOCK_RESPONSE_BUDGET")
            else:
                self.stderr.extend(chunk)
                need(len(self.stderr) <= 2048, "CLOCK_STDERR_BUDGET")
        check(deadline)

    def exchange(self, request, deadline):
        # Request is < 4096 bytes, below PIPE_BUF. No shell, no arbitrary peer command.
        check(deadline)
        need(len(request) <= 4096 and not self.output, "CLOCK_UNSOLICITED_OUTPUT")
        sent = time.monotonic_ns()
        try:
            count = os.write(self.child.stdin.fileno(), request)
        except BlockingIOError as error:
            raise ValueError("CLOCK_INPUT_BACKPRESSURE") from error
        need(count == len(request), "CLOCK_PARTIAL_WRITE")
        while b"\n" not in self.output:
            need(self.child.poll() is None or self.selector.get_map(), "CLOCK_PEER_EOF")
            self.pump(deadline)
        received = time.monotonic_ns()
        end = self.output.index(b"\n") + 1
        response = bytes(self.output[:end])
        del self.output[:end]
        need(not self.output, "CLOCK_UNSOLICITED_OUTPUT")
        check(deadline)
        return sent, received, response

    def finish(self, deadline):
        self.child.stdin.close()
        while self.child.poll() is None or self.selector.get_map():
            self.pump(deadline)
        need(not self.output and not self.stderr and self.child.returncode == 0, "CLOCK_PEER_EXIT")

    def close(self):
        self.selector.close()


def terminate(child):
    if child is None:
        return None
    if child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=1)
    return child.returncode


def capture(spec_path, output, node):
    need(spec_path.is_file() and spec_path.stat().st_size <= 16384, "CLOCK_SPEC_BUDGET")
    with open(spec_path, "rb") as stream:
        spec_raw = stream.read(16385)
    need(len(spec_raw) <= 16384, "CLOCK_SPEC_BUDGET")
    spec = spec_check(json.loads(spec_raw))
    need(node.is_absolute() and node.is_file(), "CLOCK_NODE_PATH")
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    save(output / "spec.json", spec)
    started = time.monotonic_ns()
    deadline = started + spec["wallMs"] * 1000000
    child, transport, error, session = None, None, None, None
    records = 0
    inputs_verified = False
    session_id = uuid.uuid4().hex
    token = uuid.uuid4().hex
    paths = [HERE, PEER, node, Path(sys.executable).resolve()]
    sources = None
    try:
        sources = [digest(path, deadline) for path in paths]
        clock = time.get_clock_info("monotonic")
        runtime = {"name": "python", "version": sys.version, "executable": str(paths[3]),
                   "executableSha256": sources[3]["sha256"], "componentSha256": sources[0]["sha256"]}
        initial = identity(spec["sourceOrigin"], token, runtime)
        session = dict(schemaVersion=1, kind="http-clock-correlation-session", sessionId=session_id,
                       sourceOrigin=spec["sourceOrigin"], hostIdentity=initial,
                       hostClock=dict(api="time.monotonic_ns", implementation=clock.implementation,
                                      monotonic=clock.monotonic, adjustable=clock.adjustable,
                                      resolutionSeconds=clock.resolution), nodeExecutable=sources[2],
                       components=dict(node=sources[1]["sha256"], python=sources[0]["sha256"]),
                       startedMonotonicNs=str(started))
        save(output / "session.json", session)
        check(deadline)
        child = subprocess.Popen([str(node), str(PEER), "peer", spec["sourceOrigin"], session_id],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 start_new_session=True, env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
        transport = Transport(child)
        with open(output / "samples.ndjson", "xb") as samples:
            for index in range(spec["sampleCount"]):
                check(deadline)
                before = identity(spec["sourceOrigin"], token, runtime)
                need(before == initial, "CLOCK_HOST_IDENTITY_CHANGED")
                request = raw_json(dict(schemaVersion=1, sessionId=session_id, sequence=index, nonce=uuid.uuid4().hex))
                exchange_deadline = min(deadline, time.monotonic_ns() + spec["exchangeTimeoutMs"] * 1000000)
                sent, received, response = transport.exchange(request, exchange_deadline)
                after = identity(spec["sourceOrigin"], token, runtime)
                need(after == initial, "CLOCK_HOST_IDENTITY_CHANGED")
                record = dict(sequence=index, requestWire=request.decode(), responseWire=response.decode(),
                              sendMonotonicNs=str(sent), receiveMonotonicNs=str(received), hostBefore=before, hostAfter=after)
                row = raw_json(record)
                # Reserve 32 KiB for session/result/manifest and keep failure evidence within the same budget.
                need(samples.tell() + len(row) + 32768 <= spec["maxEvidenceBytes"], "CLOCK_OUTPUT_BUDGET")
                samples.write(row)
                samples.flush()
                records += 1
                check(deadline)
                if index + 1 < spec["sampleCount"]:
                    wake = time.monotonic_ns() + spec["intervalMs"] * 1000000
                    while time.monotonic_ns() < wake:
                        check(deadline)
                        time.sleep(min(0.01, max(0, (wake - time.monotonic_ns()) / 1e9)))
            transport.finish(deadline)
        need([digest(path, deadline) for path in paths] == sources, "CLOCK_INPUT_CHANGED")
        inputs_verified = True
        check(deadline)
    except Exception as exc:
        error = str(exc)[:1024]
    finally:
        exit_code = terminate(child)
        if transport:
            transport.close()
        if child:
            for stream in (child.stdin, child.stdout, child.stderr):
                stream.close()
        if not (output / "samples.ndjson").exists():
            (output / "samples.ndjson").touch(exist_ok=False)
        if session is None:
            save(output / "session.json", dict(schemaVersion=1, kind="http-clock-correlation-session",
                                               sessionId=session_id, sourceOrigin=spec["sourceOrigin"],
                                               startedMonotonicNs=str(started), failure=error))
    result = dict(schemaVersion=1, kind="http-clock-correlation-result", sessionId=session_id,
                  status="completed" if error is None else "failed", error=error, exitCode=exit_code,
                  inputsReverified=inputs_verified, sampleCount=records, childPid=child.pid if child else None,
                  stdoutBytes=transport.stdout_bytes if transport else 0,
                  stderr=bytes(transport.stderr[:2048]).decode(errors="replace") if transport else "",
                  stderrBytes=len(transport.stderr) if transport else 0,
                  stderrSha256=hashlib.sha256(bytes(transport.stderr)).hexdigest() if transport else None,
                  finishedMonotonicNs=str(time.monotonic_ns()), performanceAccepted=False)
    save(output / "result.json", result)
    entries = []
    for path in sorted(output.iterdir()):
        raw = path.read_bytes()
        entries.append(dict(path=path.name, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
    save(output / "manifest.json", dict(schemaVersion=1, kind="http-clock-correlation-manifest", files=entries))
    need(sum(p.stat().st_size for p in output.iterdir()) <= spec["maxEvidenceBytes"], "CLOCK_OUTPUT_BUDGET")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        "The peer is the adjacent fixed Node component. All captures require a new output directory. "
        "Capture completion is transport success only; run the Node verify/map commands for evidence validation. "
        "Linux identity requires /proc time namespaces. Other hosts must explicitly use sourceOrigin=fixture."))
    sub = parser.add_subparsers(dest="command", required=True)
    command = sub.add_parser("capture", help="Freeze spec and capture bounded Python/Node causal roundtrips")
    command.add_argument("--spec", type=Path, required=True, help="Strict JSON spec; see docs/http-clock-correlation.md")
    command.add_argument("--out", type=Path, required=True, help="New directory; no overwrite or automatic resume")
    command.add_argument("--node", type=Path, required=True, help="Absolute Node executable, content hashed before/after")
    args = parser.parse_args()
    try:
        need(args.node.is_absolute(), "CLOCK_NODE_PATH")
        result = capture(args.spec, args.out, args.node.resolve())
        print(json.dumps(dict(ok=result["status"] == "completed", data=result), separators=(",", ":")))
        return 0 if result["status"] == "completed" else 6
    except Exception as error:
        print(json.dumps(dict(ok=False, error=str(error)[:1024]), separators=(",", ":")))
        return 6


def cancelled(_number, _frame):
    global CANCELLED
    CANCELLED = True


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    sys.exit(main())
