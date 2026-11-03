<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to You
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# HTTP runtime provenance probe

本组件在新实验冻结前获取独立的运行时来源事实。它读取已有 comparison attempt 的 `runtime.json`
与相邻锁，分别执行选定的 host CPython、host Node，以及固定 imageId 中的 driver CPython。
所有执行均使用固定 observer 代码和短握手；只创建一个独立 probe 容器。

```text
已有 runtime.json + runtime.lock.json          显式 probe spec
                  \                             /
                   probe：复制、验证、冻结选择与预算
                              |
       host CPython / host Node / driver CPython（独立短容器）
                              |
       observer 两帧 + 父进程实际 /proc/<pid>/exe/stat/ns
                              |
        Docker cid/image/create/limits/exit/remove 原始回执
                              |
          verify：离线重验 raw、源码、身份、锁和预算
                              |
        新实验事前冻结的 expectations / runtime-binding
```

`valid=true` 表示这份来源证明通过其证据门。`clockAlignment=unverified` 与
`performanceAccepted=false` 保持明确；本组件没有测量相位映射或性能放行逻辑。
它不会改变 comparison prepare 的离线语义，也不会启动 HugeGraph、导入图或请求 HTTP。

## 公共入口

```sh
hgbench schema http-runtime-provenance-spec
hgbench http runtime describe
hgbench http runtime probe probe-spec.json --runtime /absolute/existing-attempt/runtime.json --out /absolute/NEW_PROOF
hgbench http runtime verify /absolute/NEW_PROOF
hgbench http runtime stop /absolute/NEW_PROOF
```

独立 Python API：

```sh
python3 -B components/http/runtime-provenance.py describe
python3 -B components/http/runtime-provenance.py probe --spec probe-spec.json --runtime /absolute/attempt/runtime.json --out /absolute/NEW_PROOF
python3 -B components/http/runtime-provenance.py verify --proof /absolute/NEW_PROOF
python3 -B components/http/runtime-provenance.py stop --proof /absolute/NEW_PROOF
```

成功返回 `{ok:true,data}`。probe 不完整返回 exit 6、`ok:false`、proof 路径和诊断，部分证据保留。
verify 可以成功读取一份失败证明并返回 `valid=false/expectationsAvailable=false`；调用方必须检查这些字段。
输出目录始终新建。中断后使用原目录 verify/stop；重新执行同一 probe 会拒绝。

probe/stop 要求非 root Linux 用户，并使用本机默认 Docker Unix socket `/var/run/docker.sock`。
verify 完全离线，可在其他机器或移动完整 proof 目录后运行；若 verifier 实现已变化，必须使用兼容的
原冻结实现，源码不匹配会明确拒绝。`control/` 保存了完整 verifier 及其固定依赖。

## 严格 spec 与预算

没有实验性默认值或自由命令、shell、环境覆写、镜像拉取钩子。下面是显式短 probe 示例：

```json
{
    "schemaVersion": 1,
    "kind": "http-runtime-provenance-spec",
    "sourceOrigin": "linux-procfs",
    "budgets": {
        "wallMs": 60000,
        "commandMs": 5000,
        "handshakeMs": 10000,
        "cleanupMs": 5000,
        "maxRawBytes": 262144,
        "maxEvidenceBytes": 8388608,
        "maxExecutableBytes": 268435456,
        "maxHashBytes": 805306368
    },
    "container": {
        "cpus": 0.25,
        "memoryBytes": 134217728,
        "pids": 32,
        "tmpfsBytes": 8388608
    }
}
```

| 字段                    | 接受范围与作用                                                               |
| ----------------------- | ---------------------------------------------------------------------------- |
| `wallMs`                | 5000–120000，准备、哈希、三角色观察的共享截止                                |
| `commandMs`             | 500–15000，单 Docker 命令截止，受剩余总时间裁剪                              |
| `handshakeMs`           | 500–20000，每个绝对握手截止；`3×handshakeMs ≤ wallMs`                        |
| `cleanupMs`             | 1000–10000，清理共享截止，与主采集截止分开                                   |
| `maxRawBytes`           | 64 KiB–1 MiB，所有捕获管道和外部 proc 文件读取的累计字节；预留 32 KiB 给清理 |
| `maxEvidenceBytes`      | 1–16 MiB，包括输入 runtime、源码、raw、内容锁及派生输出                      |
| `maxExecutableBytes`    | 1–512 MiB，单次执行文件流式哈希的文件大小上限                                |
| `maxHashBytes`          | 64 MiB–4 GiB，选定文件、observer 自身 exe、外部 proc exe 的累计哈希字节      |
| `container.cpus`        | 0.1–1，仅 probe 容器硬 CPU 限制                                              |
| `container.memoryBytes` | 64–512 MiB，容器内存与 swap 总额相同                                         |
| `container.pids`        | 16–64，probe PID 上限                                                        |
| `container.tmpfsBytes`  | 4–32 MiB，唯一可写临时目录                                                   |

固定 host observer 顺序执行、流式处理文件，使用墙钟、输出、单文件和累计执行文件哈希预算；host
进程未宣称容器式 CPU/内存硬隔离。源码和证据本身的哈希工作另由证据总字节与外层时限约束。
实际输入 runtime 可能接近 2 MiB，冻结前会计算 runtime、源码、两倍 raw 和 256 KiB 封存余量；
空间不足会在派发前拒绝。容器设置只接受上述固定字段。

TypeScript 只调度该组件：总监督时间 `wallMs+cleanupMs+5000`，TERM 后再给
`cleanupMs+3000` 回收宽限。固定 worker 各自拥有短握手和独立 watchdog；外层 SSH 断开不会授权
另一份 probe。内核不可中断 I/O 沿用现有进程监督边界。

## 证明来源与隔离

host Python、host Node、Docker CLI 的文件 SHA 与大小，必须先匹配原 runtime 锁定的选择。
三角色运行期间，父进程独立读取实际 `/proc/<pid>/exe` 文件描述符、前后 stat、NSpid/UID、boot ID、
time/time_for_children/pid/mnt/user namespace 和 timens offsets。两个时刻均与固定 observer 的
ready/final 帧对应，执行文件 inode/device/摘要及进程 start ticks 必须稳定。

容器 Python 直接为 PID 1；不加入 init 或任意包装命令。其 host PID 取自同一已验证 container 的
Docker `State.Pid`，并由 NSpid 映射、实际 exe、boot/timeNS 与 observer 内部 PID 1 交叉核对。
它的 expected SHA 来自这份独立 probe 的外部 proc 读取及不可变 imageId，和待测 run 的自报 raw
保持独立来源。host 与 driver CPython 可以是不同版本，分别导出，如 3.12.3 与 3.12.15。

容器固定 `--pull=never`、network none、只读根文件系统、相同非 root UID/GID、cap-drop ALL、
no-new-privileges、无端口、无宿主 PID namespace、restart=no、log-driver=none，只读挂载固定
control 目录及受限 `/tmp`。Docker inspect 核对实际创建 argv、限制、挂载、cid、标签与 image。
目标服务与既有任务没有挂载或停止入口。

host worker 环境只含固定 PATH/locale；Python 使用 `-I -S -B`。原 image 与进程只记录以下名字的
非空存在布尔值：LD_PRELOAD、LD_AUDIT、LD_LIBRARY_PATH、NODE_OPTIONS、PYTHONHOME、PYTHONPATH、
PYTHONSTARTUP、PYTHONINSPECT、PYTHONUSERBASE。任一为 true 都拒绝可用证明。
Docker Config.Env 值和其他标签值不会写入日志或证据，不输出凭据。

取消先发布绑定 attemptId/plan SHA 的持久 stop marker，controller 在读管道和每个后续动作前检查，
停止调度并进入 finally。host 子进程 TERM 后有界 KILL/wait；容器只有在 name/cid/标签/image 都匹配时
才允许 stop/rm。清理命令共享同一截止。失败或缺权限保留 unknown/failed，不发布 expectations。
终态后的 stop 审计单独放在 `recovery/`，不能把失败证明修补成有效来源。

## 文件与离线验证

- `spec.json`、原 `runtime.json/runtime.lock.json`、派生 `plan.json/plan.lock.json`：输入选择与预算。
- `control/`：固定 controller/worker/Node helper/依赖和三份严格请求。
- `selected-files-before/after.json`：选定 host 文件与 Docker CLI 的前后身份。
- `roles/<role>.json`：原始两帧、实际 argv、外部 proc 取证、退出和捕获字节。
- `image.json`、`container-created/live.json`、`container.id`、`docker/command-*.json`：白名单 Docker 来源链。
- `result.json`：完整角色状态、失败和精确清理结果；布尔值本身不作为放行依据。
- `raw.lock.json`：上述内容的逐文件摘要与 `proofDigest`，不包含派生 binding，避免自引用。
- `runtime-binding.json`、`external-expectations.json`：仅在独立重验通过后生成。

verify 先重验输入锁与固定源码，再从两帧和外部 proc 重新导出进程身份、SHA、版本、实现、clock 和
namespace，核对同一 Docker create/inspect/正常退出/remove 链，检查原始/派生文件与源码漂移。
移动整个输入树后的结论仍由原始稳定引用决定，离线重建不访问当前 host proc 或 Docker。

raw 总量由保存的帧与外部 proc 字节逐项重算，并累加固定捕获器记录的 Docker stdout/stderr 长度。
出于环境值隐私，Docker 原始 inspection JSON 只保留白名单内容及观察到的管道长度，未保留的原文
无法离线恢复；该部分长度是锁定捕获器的审计计数。其缺项不会按零填补。
累计执行文件哈希工作精确重算为 `2×Σ(selected files bytes)+3×Σ(role exe bytes)`，包含 observer 一次
及父进程前后两次哈希。仅修改 result 的 raw/hash 数字并更新内容摘要仍会被拒绝。

## 给未来实验使用的契约

严格 `http-runtime-binding` 为：

```json
{
    "schemaVersion": 1,
    "kind": "http-runtime-binding",
    "sourceOrigin": "linux-procfs",
    "runtimeDigest": "INPUT_SELECTION_RUNTIME_LOCK_SHA256",
    "driver": {
        "imageId": "sha256:IMAGE_ID",
        "pythonExecutableSha256": "DRIVER_ACTUAL_EXE_SHA256",
        "pythonVersion": "3.12.15",
        "pythonImplementation": "cpython"
    },
    "node": {
        "executableSha256": "LOCKED_ACTUAL_NODE_EXE_SHA256",
        "version": "v22.14.0",
        "uv": "1.49.2"
    },
    "proofDigest": "RAW_LOCK_CONTENT_DIGEST"
}
```

外部 expectations 另列 `hostPythonExecutableSha256/hostPythonVersion/hostPythonImplementation`、
`actualRuntimeBinding` 的来源、固定脚本 SHA、各角色身份与 raw 引用。

`probedRuntimeDigest` 是这次输入选择的原始 runtime-lock 摘要，供审计引用。未来 comparison 的完整
runtimeDigest 会随计划与 source 改变。新冻结流程必须逐项匹配 imageId、host/driver 的实际 exe SHA
与版本、Node/uv、注入标志和相关固定 source，并把 proofDigest 与原选择关联保存在新锁中。
不能要求两个完整 runtimeDigest 相等，也不能把旧 binding 的 runtimeDigest 原样贴作新 run 的身份。
这个未来冻结关联属于后续集成；当前 probe 只提供可独立核验的事实。

在旧 run 之后生成的 probe，不构成旧 run 的事前独立 expected 来源。probe 的 clock/timeNS 仅描述
probe 自己的执行；任何实验仍需其自己的原始运行时证明、相位关联和校准门。

本地专项采用合成 Linux 证明做协议与篡改测试，另在 Linux 下实际执行固定 host worker 检查外部 proc
文件描述符绑定。它们不替代专用机器上的真实 Docker probe 或性能校准。
