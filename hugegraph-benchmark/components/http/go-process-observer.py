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
"""Host-side observation of one held Go process. Imported by bounded controllers."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hgbench_go_clock_snapshot", HERE / "go-clock-probe.py")
CLOCK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLOCK)


def need(value, message):
    if not value:
        raise ValueError("GO_PROCESS: " + message)


def regular(path, cap):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode) and before.st_size <= cap, "marker file type/bound")
        raw = stream.read(cap + 1)
        after = os.fstat(stream.fileno())
    need(len(raw) == before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns,
         "marker changed while reading")
    return raw


def atomic(path, raw):
    path = Path(path)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    finally:
        temporary.unlink()


def proc_status(path):
    # Linux procfs reports size zero for nonempty virtual files.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        need(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "proc status type")
        raw = stream.read(65537)
    need(len(raw) <= 65536, "proc status bound")
    return raw.decode()


def observe(marker_file, release_file, output_file, binary, container_id, docker):
    """docker(args) must use its caller's frozen executable, deadline/output and cancellation bounds.

    Does not search the host PID tree. Reads only Docker's exact container members;
    the Go PID remains held until this function writes its exact nonce release.
    Failure writes a partial receipt and never releases HTTP admission.
    """
    result = {"schemaVersion": 1, "kind": "http-go-live-process-observation",
              "containerId": container_id, "error": None, "performanceAccepted": False,
              "sourceSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "clockSnapshotSourceSha256": hashlib.sha256((HERE / "go-clock-probe.py").read_bytes()).hexdigest()}
    try:
        need(re.fullmatch(r"[a-f0-9]{64}", container_id), "invalid container ID")
        raw = regular(marker_file, 65536)
        marker = json.loads(raw)
        result["markerRaw"] = raw.decode("utf-8", errors="strict")
        need(marker.get("schemaVersion") == 1 and marker.get("kind") == "http-go-live-process-marker" and
             marker.get("stage") in ("ready", "finished") and re.fullmatch(r"[a-f0-9]{32}", marker.get("nonce", "")),
             "marker protocol")
        for key, expected in (("binarySha256", binary["sha256"]), ("sourceSha256", binary["sourceSha256"]),
                              ("goVersion", binary["goVersion"]), ("goos", "linux"), ("goarch", binary["goarch"])):
            need(marker["binary"].get(key) == expected, "marker binary identity")
        started = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        result["hostBeforeNs"] = str(started)
        info = json.loads(docker(["inspect", container_id]))[0]
        result["container"] = {"id": info["Id"], "image": info["Image"], "running": info["State"]["Running"],
                               "rootHostPid": info["State"]["Pid"]}
        need(info["Id"] == container_id and info["State"]["Running"], "container not live")
        root_namespace = os.readlink(Path("/proc") / str(info["State"]["Pid"]) / "ns/pid")
        lines = docker(["top", container_id, "-eo", "pid"]).splitlines()
        need(lines and lines[0].strip() == "PID" and len(lines) <= 4097, "Docker PID inventory")
        matches = []
        clock = marker["clock"]
        for line in lines[1:]:
            need(re.fullmatch(r"\s*[0-9]+\s*", line), "Docker PID format")
            pid = int(line)
            proc = Path("/proc") / str(pid)
            try:
                if os.readlink(proc / "ns/pid") != root_namespace:
                    continue
                status = proc_status(proc / "status")
                nspid = next(row.split()[1:] for row in status.splitlines() if row.startswith("NSpid:"))
                if int(nspid[-1]) != clock["pid"]:
                    continue
                observed = CLOCK.process_identity(pid)
                if observed["startTicks"] == clock["process"]["startTicks"]:
                    matches.append(observed)
            except FileNotFoundError:
                continue  # A different container member can exit during enumeration.
        need(len(matches) == 1, "held Go PID not uniquely mapped")
        observed = matches[0]
        result["processBefore"] = observed
        need(observed["executableSha256"] == binary["sha256"], "actual executable SHA differs")
        for key in ("bootId", "startTicks", "pidNamespace", "mntNamespace", "timeNamespace", "time_for_childrenNamespace", "cgroupNamespace"):
            need(observed[key] == clock["process"][key], "actual Go process identity differs")
        result["host"] = CLOCK.process_identity(os.getpid())
        need(result["host"]["bootId"] == observed["bootId"] and
             result["host"]["timeNamespace"] == observed["timeNamespace"], "host clock domain differs")
        result["processAfter"] = CLOCK.process_identity(observed["hostPid"])
        need(result["processAfter"] == observed, "held process identity changed")
        result["hostAfterNs"] = str(time.clock_gettime_ns(time.CLOCK_MONOTONIC))
        ack = {"schemaVersion": 1, "kind": "http-go-live-process-release", "id": marker["id"],
               "stage": marker["stage"], "nonce": marker["nonce"], "markerSha256": hashlib.sha256(raw).hexdigest()}
        release = (json.dumps(ack, separators=(",", ":")) + "\n").encode()
        result["releaseRaw"] = release.decode()
        # Persist the independent identity before allowing the held process to proceed.
        atomic(output_file, (json.dumps(result, indent=2) + "\n").encode())
        atomic(release_file, release)
        return result
    except Exception as error:
        result["error"] = type(error).__name__ + ": " + str(error)[:1000]
        if not Path(output_file).exists():
            atomic(output_file, (json.dumps(result, indent=2) + "\n").encode())
        raise
