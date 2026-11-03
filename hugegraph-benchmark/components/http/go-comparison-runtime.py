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
"""Go-specific comparison runtime extension; the controller owns all Docker work."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re

CONTROL_FILES = ("go-comparison-runtime.py", "go-process-observer.py", "go-clock-probe.py")
BINARY_FIELDS = {"path", "sha256", "sourceSha256", "goVersion", "goos", "goarch"}


class GoRuntimeError(ValueError):
    pass


def need(value, message):
    if not value:
        raise GoRuntimeError("GO_RUNTIME: " + message)


def path(value):
    need(isinstance(value, str) and value.startswith("/") and len(value) <= 2048 and
         not any(ord(c) < 32 or c == "," for c in value), "clean absolute path required")


def validate(executor):
    need(isinstance(executor, dict) and set(executor) == {"engine", "binary", "clockProof"} and
         executor["engine"] == "go-http-v1", "executor fields/engine")
    binary = executor["binary"]
    need(isinstance(binary, dict) and set(binary) == BINARY_FIELDS, "binary fields")
    for field in ("sha256", "sourceSha256"):
        need(isinstance(binary[field], str) and re.fullmatch(r"[a-f0-9]{64}", binary[field]), "binary digest")
    need(binary["goVersion"] == "go1.27.3" and binary["goos"] == "linux" and
         binary["goarch"] in ("amd64", "arm64"), "Go/platform")
    path(binary["path"])
    path(executor["clockProof"])
    # The binary is mounted at its frozen path; it must not hide controller mounts.
    for reserved in ("/source", "/node", "/comparison", "/control", "/evidence", "/proc", "/sys", "/dev", "/tmp"):
        need(not (binary["path"] == reserved or binary["path"].startswith(reserved + "/")), "binary mount collision")


def bind_profile(runtime, profile):
    validate(runtime["executor"])
    need(profile.get("schemaVersion") == 2 and profile.get("executor", {}).get("hostIdentityGate") is True,
         "runtime v2 requires Go profile with host identity gate")
    need(profile["executor"]["binary"] == runtime["executor"]["binary"], "profile/runtime binary differs")
    need(profile["executor"]["cpuQuotaCores"] == runtime["driver"]["cpus"] and
         profile["executor"]["maxRssBytes"] == runtime["driver"]["memoryBytes"], "profile/driver resource limits differ")
    need(runtime["executor"]["binary"]["path"] != runtime["driver"]["python"], "binary hides driver supervisor")


def binary_source_identity(source_inventory, binary):
    prefix = "components/http-go/"
    files = [{"path": f["path"][len(prefix):], "bytes": f["bytes"], "sha256": f["sha256"]}
             for f in source_inventory if f.get("type") == "file" and f["path"].startswith(prefix) and
             (f["path"].endswith(".go") or f["path"] == prefix + "go.mod")]
    files.sort(key=lambda f: f["path"])
    need(0 < len(files) <= 64 and sum(f["bytes"] for f in files) <= 8 * 1024 * 1024, "Go source inventory bound")
    digest = hashlib.sha256(json.dumps(files, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
    need(digest == binary["sourceSha256"], "binary does not match frozen Go source inventory")
    return digest


def gate_locations(profile):
    cases = len(profile["cases"])
    result = []
    if profile.get("serviceChecks"):
        for phase in ("preflight", "postcheck"):
            result.extend((f"{phase}/p{i + 1:04d}.json.identity", f"p{i + 1:04d}") for i in range(cases))
    result.extend((f"runs/r{i + 1:04d}.json.identity", f"r{i + 1:04d}") for i in
                  range(cases * len(profile["loads"]) * profile["repeats"]))
    need(len(result) <= 5120, "gate inventory bound")
    return result


class ProcessObserver:
    def __init__(self, profile, container_id, evidence, output, docker, guard):
        self.binary = profile["executor"]["binary"]
        self.locations = gate_locations(profile)
        self.container_id, self.evidence, self.output = container_id, Path(evidence), Path(output)
        self.docker, self.guard = docker, guard
        self.seen = set()
        self.host_processes = set()
        self.namespace_processes = set()
        self.logical_processes = {}
        spec = importlib.util.spec_from_file_location("go_live_process_observer", Path(__file__).with_name("go-process-observer.py"))
        self.observer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.observer)

    def poll(self):
        for relative, expected_id in self.locations:
            for stage in ("ready", "finished"):
                key = (relative, stage)
                marker = self.evidence / relative / (stage + ".json")
                if key in self.seen or not marker.exists():
                    continue
                self.guard()
                raw = self.observer.regular(marker, 65536)
                value = json.loads(raw)
                need(value.get("id") == expected_id and value.get("stage") == stage, "unexpected logical gate identity")
                need(stage == "ready" or (relative, "ready") in self.seen, "finished gate preceded ready observation")
                clock = value["clock"]
                process_key = (clock["pid"], clock["process"]["startTicks"], clock["process"]["pidNamespace"])
                if stage == "ready":
                    need(process_key not in self.namespace_processes, "Go process reused for distinct logical run")
                else:
                    need(process_key == self.logical_processes[relative], "Go process changed between gates")
                folder = self.output / relative
                folder.mkdir(parents=True, exist_ok=True)
                receipt = self.observer.observe(marker, marker.with_name(stage + "-release.json"),
                                                folder / (stage + "-host.json"), self.binary,
                                                self.container_id, self.docker)
                process = receipt["processBefore"]
                identity = (process["hostPid"], process["startTicks"])
                if stage == "ready":
                    self.host_processes.add(identity)
                    self.namespace_processes.add(process_key)
                    self.logical_processes[relative] = process_key
                self.seen.add(key)

    def finish(self):
        self.poll()
        need(len(self.seen) == 2 * len(self.locations), "incomplete live process observation inventory")
        return {"schemaVersion": 1, "kind": "http-go-comparison-observer-summary", "gateCount": len(self.seen),
                "processCount": len(self.host_processes), "performanceAccepted": False}
