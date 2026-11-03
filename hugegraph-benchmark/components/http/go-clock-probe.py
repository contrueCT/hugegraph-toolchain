#!/usr/bin/env python3
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
"""Bounded Go/container/host CLOCK_MONOTONIC exchanges. No HTTP requests."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import subprocess
import sys
import time
import uuid

CONTRACT = "linux-go-monotonic-host-bracket-v1"
ENV = {"PATH": "/usr/bin:/bin", "HOME": str(Path.home()), "LANG": "C", "LC_ALL": "C"}
NAMESPACES = ("pid", "mnt", "time", "time_for_children", "cgroup")


def sha(path, limit=32 * 1024 * 1024):
    with open(path, "rb") as stream:
        h = hashlib.sha256()
        size = 0
        while True:
            part = stream.read(65536)
            if not part:
                return h.hexdigest()
            size += len(part)
            if size > limit:
                raise ValueError("identity file exceeds bound")
            h.update(part)


def process_identity(pid):
    root = Path("/proc") / str(pid)
    stat = (root / "stat").read_text()
    fields = stat[stat.rfind(")") + 1:].split()
    status = (root / "status").read_text()
    nspid = next(line.split()[1:] for line in status.splitlines() if line.startswith("NSpid:"))
    result = {
        "hostPid": pid, "namespacePids": [int(x) for x in nspid],
        "startTicks": fields[19],
        "bootId": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "timeOffsets": (root / "timens_offsets").read_text().strip(),
        "executableSha256": sha(root / "exe"),
    }
    for name in NAMESPACES:
        result[name + "Namespace"] = os.readlink(root / "ns" / name)
    return result


def probe(binary, image, output):
    if sys.platform != "linux" or not re.fullmatch(r"sha256:[a-f0-9]{64}", image):
        raise ValueError("requires Linux and a locally available immutable Docker image ID")
    binary = Path(binary).resolve(strict=True)
    binary_sha = sha(binary)
    if not os.access(binary, os.X_OK):
        raise ValueError("binary must be executable")
    out = Path(output).resolve()
    out.mkdir(mode=0o700)
    source = Path(__file__).resolve().read_bytes()
    (out / "producer.py").write_bytes(source)
    owner = uuid.uuid4().hex
    name = "hgbench-go-clock-" + owner
    deadline = time.monotonic() + 30
    proof = {"schemaVersion": 1, "kind": "http-go-host-clock-proof", "contract": CONTRACT,
             "binarySha256": binary_sha, "imageId": image, "owner": owner,
             "sourceSha256": hashlib.sha256(source).hexdigest(), "exchanges": [],
             "error": None, "performanceAccepted": False}
    def cancelled(signum, frame):
        raise InterruptedError("clock probe interrupted by signal " + str(signum))

    handlers = {sig: signal.signal(sig, cancelled) for sig in (signal.SIGINT, signal.SIGTERM)}
    cid = None
    child = None

    def command(args, maximum=4 * 1024 * 1024, check=True):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("whole clock-probe deadline expired")
        r = subprocess.run(args, capture_output=True, timeout=min(20, remaining), env=ENV)
        if len(r.stdout) + len(r.stderr) > maximum:
            raise ValueError("command output exceeds bound")
        if check and r.returncode:
            raise RuntimeError("bounded command failed: " + args[0])
        return r

    def inspect():
        return json.loads(command(["docker", "inspect", cid or name]).stdout)[0]

    try:
        proof["host"] = process_identity(os.getpid())
        info = time.get_clock_info("monotonic")
        proof["host"]["pythonVersion"] = platform.python_version()
        proof["host"]["clock"] = {"implementation": info.implementation, "monotonic": info.monotonic,
                                    "adjustable": info.adjustable, "resolution": info.resolution,
                                    "sampleFunction": "time.clock_gettime_ns(time.CLOCK_MONOTONIC)"}
        create = ["docker", "create", "--pull", "never", "--name", name, "--label", "org.apache.hugegraph.hgbench.goClock=" + owner,
                  "--interactive", "--log-driver", "none", "--network", "none", "--cpus", "2",
                  "--memory", "256m", "--memory-swap", "256m", "--pids-limit", "64", "--user", "1000:1000",
                  "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
                  "--mount", "type=bind,src=" + str(binary) + ",dst=/hgbench-http-go,readonly",
                  "--env", "GODEBUG=", "--env", "GOGC=100", "--env", "GOMEMLIMIT=off", "--env", "GOMAXPROCS=2",
                  "--entrypoint", "/hgbench-http-go", image, "clock-probe"]
        proof["createCommand"] = create
        cid = command(create).stdout.decode().strip()
        if not re.fullmatch(r"[a-f0-9]{64}", cid):
            raise ValueError("invalid Docker container identity")
        proof["containerId"] = cid
        with open(out / "stderr.txt", "xb") as stderr:
            child = subprocess.Popen(["docker", "start", "--attach", "--interactive", cid],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, bufsize=0, env=ENV)
            buffer = bytearray()
            selector = selectors.DefaultSelector()
            selector.register(child.stdout, selectors.EVENT_READ)

            def line():
                while b"\n" not in buffer:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(min(2, remaining)):
                        if remaining <= 0:
                            raise TimeoutError("clock exchange deadline expired")
                        continue
                    part = os.read(child.stdout.fileno(), 65536)
                    if not part:
                        raise ValueError("clock exchange unexpected EOF")
                    buffer.extend(part)
                    if len(buffer) > 65536:
                        raise ValueError("clock response exceeds bound")
                split = buffer.index(b"\n") + 1
                raw = bytes(buffer[:split])
                del buffer[:split]
                return raw.decode("utf-8", errors="strict")

            try:
                proof["readyRaw"] = line()
                proof["containerBefore"] = inspect()
                pid = proof["containerBefore"]["State"]["Pid"]
                if not pid:
                    raise ValueError("Go container exited before host identity observation")
                proof["processBefore"] = process_identity(pid)
                for sequence in range(5):
                    if sequence == 4:
                        proof["processAfter"] = process_identity(pid)
                        proof["containerAfterExchanges"] = inspect()
                    request = {"schemaVersion": 1, "kind": "finish" if sequence == 4 else "sample",
                               "sequence": sequence, "nonce": uuid.uuid4().hex}
                    raw = json.dumps(request, separators=(",", ":")) + "\n"
                    before = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
                    child.stdin.write(raw.encode())
                    response = line()
                    after = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
                    proof["exchanges"].append({"requestRaw": raw, "responseRaw": response,
                                               "hostBeforeNs": str(before), "hostAfterNs": str(after)})
                child.stdin.close()
                proof["exitCode"] = child.wait(timeout=max(0.1, deadline - time.monotonic()))
                extra = child.stdout.read(65537)
                proof["trailingOutputBytes"] = len(buffer) + len(extra)
            finally:
                selector.close()
        proof["containerTerminal"] = inspect()
    except Exception as error:
        proof["error"] = type(error).__name__ + ": " + str(error)[:1000]
    finally:
        # Cleanup has its own five-second reserve and touches only this exact owned container.
        deadline = max(deadline, time.monotonic() + 5)
        if "createCommand" in proof:
            try:
                current = inspect()
                cid = current["Id"]
                if current["Config"]["Labels"].get("org.apache.hugegraph.hgbench.goClock") != owner:
                    raise ValueError("cleanup ownership mismatch")
                removed = command(["docker", "rm", "--force", cid])
                proof["cleanup"] = {"removed": removed.returncode == 0,
                                    "absent": command(["docker", "inspect", cid], check=False).returncode != 0}
            except Exception as error:
                proof["cleanup"] = {"removed": False, "error": str(error)[:1000]}
        if child:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=2)
            for stream in (child.stdin, child.stdout):
                if stream and not stream.closed:
                    stream.close()
        raw = (json.dumps(proof, indent=2) + "\n").encode()
        if len(raw) > 1024 * 1024:
            raise ValueError("proof exceeds output budget")
        (out / "proof.json").write_bytes(raw)
        members = []
        for file in sorted(out.iterdir()):
            if file.is_file():
                members.append({"path": file.name, "bytes": file.stat().st_size, "sha256": sha(file)})
        (out / "raw.lock.json").write_text(json.dumps({"schemaVersion": 1, "kind": "http-go-clock-lock",
                                                       "files": members}, indent=2) + "\n")
        for sig, previous in handlers.items():
            signal.signal(sig, previous)
    print(json.dumps({"directory": str(out), "error": proof["error"], "cleanup": proof.get("cleanup"),
                      "performanceAccepted": False}))
    return 1 if proof["error"] or not proof.get("cleanup", {}).get("absent") else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, help="Explicit executable Go component; SHA retained")
    parser.add_argument("--image-id", required=True, help="Local immutable Docker sha256 ID; no download")
    parser.add_argument("--out", required=True, help="New evidence directory; no overwrite")
    args = parser.parse_args()
    return probe(args.binary, args.image_id, args.out)


if __name__ == "__main__":
    sys.exit(main())
