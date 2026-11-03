/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { readFile, writeFile, mkdir, rm } from "node:fs/promises";
import { join, dirname, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
const run = promisify(execFile),
    hash = (b) => createHash("sha256").update(b).digest("hex");
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = async (path, value) => {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, JSON.stringify(value, null, 2) + "\n");
};

/** Synthetic Docker/proc snapshots plus actual Python collector output over a virtual-clock procfs fixture. */
export async function lifecycleFixture({
    folder,
    env,
    runtime,
    execution,
    arm,
    start,
    targetIdentity,
    targetContainer,
    simulateLinux = false,
}) {
    const controller = join(folder, "controller"),
        root = dirname(dirname(dirname(env.root)));
    const d = runtime.settings.driver,
        uid = env.owner.uid,
        gid = env.owner.gid;
    const id = (1001 + arm.position).toString(16).padStart(64, "0");
    const targetPid = 10000 + arm.position,
        driverPid = 20000 + arm.position,
        supervisorPid = 30000 + arm.position;
    const targetMember = `/system.slice/docker-${targetContainer}.scope`,
        driverMember = `/system.slice/docker-${id}.scope`;
    const namespaces = { pid: "pid:[5]", net: targetIdentity.namespaces.net, mnt: "mnt:[6]", user: "user:[4]" };
    function proc(role, hostPid, innerPid, ticks, member, ns) {
        return {
            role,
            hostPid,
            statRaw: `${hostPid} (fixture (worker)) ` + ["S", ...Array(18).fill("0"), ticks, "0"].join(" "),
            statusRaw: `Name:\tfixture\nUid:\t${uid}\t${uid}\t${uid}\t${uid}\nGid:\t${gid}\t${gid}\t${gid}\t${gid}\nNSpid:\t${hostPid}\t${innerPid}\n`,
            cgroupRaw: `0::${member}\n`,
            namespaces: ns,
        };
    }
    const proof = {
        schemaVersion: 1,
        kind: "http-comparison-process-identities",
        hostBootId: execution.hostBootId,
        recordedMonotonicNs: String(start + 2200000000),
        roles: [
            proc(
                "target",
                targetPid,
                targetIdentity.pid,
                String(targetIdentity.startTimeTicks),
                targetMember,
                targetIdentity.namespaces,
            ),
            proc("driver", driverPid, 1, "2000", driverMember, namespaces),
            proc("driver-supervisor", supervisorPid, 2, "3000", driverMember, namespaces),
        ],
    };
    await put(join(controller, "process-identities.json"), proof);
    const label = "org.apache.hugegraph.hgbench.comparison";
    const before = {
        Id: id,
        Name: `/hgbench-pair-${execution.executionId.slice(0, 12)}-${arm.runId.slice(-24)}`,
        Image: d.image,
        Config: {
            User: `${uid}:${gid}`,
            Labels: {
                [label + ".execution"]: execution.executionId,
                [label + ".run"]: arm.runId,
                [label + ".runtime"]: execution.runtimeDigest,
            },
            Entrypoint: [d.python],
            Cmd: ["-B", "/control/comparison-workflow.py", "_driver", "--arm", arm.arm, "--run-id", arm.runId],
            WorkingDir: "/source",
            Hostname: id.slice(0, 12),
        },
        HostConfig: {
            NetworkMode: "container:" + targetContainer,
            ReadonlyRootfs: true,
            Memory: d.memoryBytes,
            MemorySwap: d.memoryBytes,
            NanoCpus: d.cpus * 1e9,
            PidsLimit: d.pids,
            Init: true,
            CapDrop: ["ALL"],
            SecurityOpt: ["no-new-privileges:true"],
            RestartPolicy: { Name: "no" },
            LogConfig: { Type: "none" },
            Tmpfs: { "/tmp": `rw,nosuid,nodev,noexec,size=${d.tmpfsBytes}` },
        },
        Mounts: [
            ["/source", runtime.settings.sourceRoot, false],
            ["/node", runtime.settings.nodeHome, false],
            ["/comparison", root + "/comparison", false],
            ["/control", root, false],
            ["/evidence", `${root}/work/${arm.runId}/driver`, true],
        ].map(([Destination, Source, RW]) => ({ Type: "bind", Destination, Source, RW })),
        State: { Status: "created", Running: false },
    };
    const running = structuredClone(before);
    running.State = {
        Status: "running",
        Running: true,
        Pid: driverPid,
        StartedAt: new Date(1700000000000 + start / 1e6).toISOString(),
    };
    const final = structuredClone(running);
    final.State = {
        ...running.State,
        Status: "exited",
        Running: false,
        OOMKilled: false,
        ExitCode: 0,
        FinishedAt: new Date(1700000008000 + start / 1e6).toISOString(),
    };
    for (const [name, value] of [
        ["before-start", before],
        ["started", running],
        ["final", final],
    ])
        await put(join(controller, `driver-${name}.json`), value);
    await put(join(controller, "driver-ready.json"), {
        runId: arm.runId,
        atMonotonicNs: String(start + 2100000000),
        pid: 2,
        startTimeTicks: "3000",
        bootId: execution.hostBootId,
    });
    await put(join(controller, "go.json"), {
        runId: arm.runId,
        runtimeDigest: execution.runtimeDigest,
        atMonotonicNs: String(start + 2800000000),
    });
    await put(join(controller, "release.json"), { runId: arm.runId, atMonotonicNs: String(start + 7100000000) });
    const workflow = {
        runId: arm.runId,
        ok: true,
        taskId: "fixture-task",
        startedMonotonicNs: String(start + 3000000000),
        finishedMonotonicNs: String(start + 5000000000),
        performanceAccepted: false,
    };
    await put(join(controller, "workflow-result.json"), workflow);
    const input = {
        root,
        folder,
        start,
        simulateLinux,
        bootId: execution.hostBootId,
        proof,
        limits: { target: env.settings.budget, driver: d },
        collector: runtime.settings.collector,
        producer: resolve("tests-normative/test_http_resource_collector.py"),
    };
    await put(join(folder, "fixture-input.json"), input);
    await run(
        "python3",
        [
            "-B",
            "-c",
            `
import importlib.util,json,sys,shutil
from unittest.mock import patch
from pathlib import Path
v=json.load(open(sys.argv[1])); spec=importlib.util.spec_from_file_location('fixture',v['producer'])
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);C=m.C
base=Path(v['folder'])/'fixture-kernel';base.mkdir()
proc,group,unused=m.fixture(base)
prototype=base/'prototype';shutil.copytree(group,prototype);shutil.rmtree(group)
for role in v['proof']['roles'][:2]:
 name=role['role']; member=role['cgroupRaw'].strip()[3:]; dst=base/'cgroup'/member.lstrip('/')
 shutil.copytree(prototype,dst); p=proc/str(role['hostPid']);p.mkdir()
 (p/'stat').write_text(role['statRaw']);(p/'cgroup').write_text(role['cgroupRaw'])
 limit=v['limits'][name]
 (dst/'cpu.max').write_text(str(round(limit['cpus']*100000))+' 100000\\n')
 (dst/'memory.max').write_text(str(limit['memoryBytes'])+'\\n');(dst/'pids.max').write_text(str(limit['pids'])+'\\n')
 if name=='driver':(dst/'io.stat').unlink()
(proc/'sys/kernel/random/boot_id').write_text(v['bootId'])
stop=Path(v['folder'])/'controller/collector-stop.json'
s={'schemaVersion':1,'kind':'http-resource-collection',**v['collector'],'filesystemPath':v['root'],
 'roles':[{'role':r['role'],'hostPid':r['hostPid']} for r in v['proof']['roles'][:2]],
 'controlledStop':{'path':str(stop),'token':'a'*32}}
class Clock:
 now=v['start']+2500000000
 def mono(self):self.now+=1000;return self.now
 def sleep(self,seconds):self.now+=int(seconds*1e9)
clock=Clock();C.time.monotonic_ns=clock.mono;C.time.sleep=clock.sleep
# This synthetic proc tree uses a virtual clock and boot identity. Never mix
# the real test host's /proc/self clock proof into these integration records.
clock_identity=C.collector_clock_identity
def synthetic_clock_identity(origin,hash_executable):
 with patch.object(C.sys,'platform','synthetic-fixture'):
  return clock_identity(origin,hash_executable)
C.collector_clock_identity=synthetic_clock_identity
class K(C.Kernel):
 calls=0
 def host(self,filesystem):
  self.calls+=1
  if self.calls==4:
   C.write_new(stop,{'schemaVersion':1,'kind':'http-resource-stop','token':'a'*32,'specSha256':C.sha(C.json_bytes(s)),
    'hostBootId':v['bootId'],'workflowFinishedMonotonicNs':str(v['start']+5000000000),
    'stopRequestedMonotonicNs':str(v['start']+5100000000)})
  return super().host(filesystem)
out=Path(v['folder'])/'collector';kernel=K(proc,fixture=True)
if v.get('simulateLinux'):kernel.origin='linux-procfs' # Synthetic integration contract record, never live evidence.
r=C.collect(s,out,kernel=kernel)
assert r['status']=='completed',r
shutil.rmtree(base)
`,
            join(folder, "fixture-input.json"),
        ],
        { timeout: 10000 },
    );
    await rm(join(folder, "fixture-input.json"));
    const spec = await read(join(folder, "collector/spec.json")),
        summary = await read(join(folder, "collector/summary.json"));
    const stop = await read(join(controller, "collector-stop.json"));
    await put(join(controller, "collector-roles.json"), {
        runId: arm.runId,
        targetJavaHostPid: targetPid,
        driverInitHostPid: driverPid,
        spec,
    });
    await put(join(controller, "collector-window.json"), {
        runId: arm.runId,
        workflowStartedMonotonicNs: workflow.startedMonotonicNs,
        workflowFinishedMonotonicNs: workflow.finishedMonotonicNs,
        stopMarker: stop,
        summarySha256: hash(await readFile(join(folder, "collector/summary.json"))),
        firstSampleStartedMonotonicNs: summary.firstSampleStartedMonotonicNs,
        lastSampleFinishedMonotonicNs: summary.lastSampleFinishedMonotonicNs,
        measurementPhaseAbsoluteAnchors: false,
        performanceAccepted: false,
    });
    return { driverId: id, workflow, spec, summary, proof };
}
