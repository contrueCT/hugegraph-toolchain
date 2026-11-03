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

# HTTP 原始证据到配对统计

`paired-evidence.mjs` 是离线适配器。它读取预冻结 comparison plan 和一次执行的密封快照，
重新运行 HTTP analyzer、核验运行环境记录、原始 JVM 栈、driver 和资源样本，再生成配对统计输入及离线 HTML。
它不启动容器、不发送 HTTP、不补跑失败臂，也不批准性能结论。

```text
冻结的 comparison plan ──┐
                         ├─ 内容锁 + 原始证据重建 ── observations.json ── paired/core
预注册 execution 快照 ───┘                      └─ verification.json    └─ analysis.json
```

```sh
node components/http/paired-evidence.mjs --help
node components/http/paired-evidence.mjs \
  --plan /path/to/comparison-plan \
  --execution /path/to/sealed-execution \
  --out /path/to/new-analysis
```

输出目录必须不存在。退出 0 表示转换完成，调用者仍须检查 `data.valid`（证据有效性）和
`data.statisticsEligible`（预声明样本/统计计算条件是否满足）；存在缺臂等质量问题时会保留
所有计划位置并输出 invalid。无法可信读取计划/密封清单时退出 6。该版本始终 `performanceAccepted=false`。

## 预冻结共享输入

planner 的共享输入是内容锁；适配器增加以下最小语义约束，供受控执行器使用。

- `build.json`：当前构建产物的完整 `distribution.lock.json`，包含 `sourceCommit`（40 位小写 SHA）、
  `distribution`（绝对路径）、`files: [{path, bytes, sha256}]`、`testsRun`（boolean）。
  全部常规文件须与环境 plan 的 distribution inventory 逐项一致。
- `settings.json`：`hugegraph-http-environment-settings` 完整对象，移除 `mode`。
  每臂注入计划指定 mode 后与实际环境 plan 精确一致；路径须在冻结前归一为真实路径。
- `environment.json`：以下类型化主机快照。bootId 与模式探针一致，镜像 ID 与 settings/inspect 一致，
  Node 版本与原始 HTTP implementation 一致。其余字段保留为环境声明，干扰是否可接受另行验收。

```json
{
  "schemaVersion": 1,
  "kind": "http-comparison-host-environment",
  "hostBootId": "00000000-0000-0000-0000-000000000001",
  "kernel": {"release": "actual-kernel-release", "architecture": "x86_64"},
  "cpu": {"model": "actual-cpu-model", "logicalProcessors": 16},
  "memoryBytes": 34359738368,
  "docker": {"serverVersion": "actual-version", "imageId": "sha256:<64-hex-image-id>"},
  "node": {"version": "v22.0.0"},
  "coRunningJobs": []
}
```

镜像/Node 版本应填写实际采集值。`coRunningJobs=[]` 本身不证明测量窗口独占；采集器证据、A/A、
已知回归检出、饱和曲线和 driver headroom 仍需独立验收。

## 控制器执行快照协议 v1

控制器在首次 dispatch 前写入独占、不可覆盖的 registration；同一 attempt 继续 status/stop，不得重放。
完成后归档以下布局。只收集原始证据、控制文件、配置和生命周期记录，不复制 RocksDB 数据/JDK/发行版。

```text
execution/
  execution.json
  terminal.json
  execution.lock.json
  arms/<planned-runId>/
    receipt.json
    controller/runtime.json, controller/runtime.lock.json
    raw/                         # 完整 HTTP workflow output，含 raw.lock.json
    environment/
      plan.json, plan.lock.json
      hugegraph-{environment,service,mode-probe}.py
      conf/...                   # plan.controlFiles 的全部内容
      container-before-start.json, container-final.json, supervisor-result.json
      work/ready.json, work/service-result.json
      work/mode-pre/...           # 探针 request/result/attach/thread-dump 原始文件
      work/mode-post/...
```

`execution.json` 的内容由控制器形成；缺臂可以从 receipts 缺失，禁止用新的执行替换原 runId：

```json
{
  "schemaVersion": 1,
  "kind": "http-comparison-execution",
  "executionId": "<32-hex-controller-execution-id>",
  "planDigest": "<plan.json planDigest>",
  "planContentDigest": "<plan.lock.json contentDigest>",
  "hostBootId": "<same-host-boot-uuid>",
  "registeredMonotonicNs": "1000000000",
  "runtimeDigest": "<SHA-256 of runtime.lock.json original bytes>",
  "receipts": [
    {"runId": "<planned-runId>", "path": "arms/<planned-runId>/receipt.json"}
  ]
}
```

`runtimeDigest` 必填，并在每臂 `controller/` 归档相同的两个文件：

```json
{
  "schemaVersion": 1,
  "kind": "http-comparison-runtime-lock",
  "planDigest": "<same-planDigest>",
  "runtime": {"path": "runtime.json", "bytes": 1234, "sha256": "<64-hex>"}
}
```

`runtime.json` 为执行器冻结的 `http-comparison-runtime-plan`，包含同一 `planDigest`、
`planContentDigest`、`componentSha256`、完整 `settings` 和 `identities`。
`identities.source`、`identities.node` 是完整源代码和 Node 目录清单，常规文件包含
`{path, type: "file", mode, bytes, sha256}`；其中源文件相对路径如 `components/http/runner.mjs`，
Node 可执行文件为 `bin/node`。`hostPython`、`dockerExecutable` 各含绝对 `path/bytes/sha256`。
适配器把七个 HTTP 实现文件（含 direct-phase-clock）和 Node 可执行文件与该锁关联；全量资产在真实 dispatch 前的内容验证由
执行器完成。每臂重复归档确保每个失败臂也保留它采用的执行器条件。

新 run/probe 的 `phaseClock` 和 `phases[].clock` 由 analyzer 重验，缺失 session、绑定/时间顺序/elapsed
矛盾或预算不符都会失败。旧数据同时没有这些字段时明确 unavailable。原始区间不自动升级为宿主时间。
每臂 HTML 展示 `phaseClocks` 与可选 JVM 观测；JVM 启用后独立核 launch、capture、实际映射/JDK，
使用身份匹配的 parser 在有界临时目录重解析，并逐成员比较原封输出。摘要成功字段不能替代重解析。
`clockAlignment`、phase 归因与观测开销仍单独保留为未验证/待校准。

每臂 receipt 示例。所有新时间字段使用 **十进制字符串**，单位纳秒，同一个 Linux boot monotonic clock。
旧 Python probe 的 JSON number 只有在 `Number.isSafeInteger` 为真时接受；超过 `2^53-1` 拒绝转换。

```json
{
  "schemaVersion": 1,
  "kind": "http-comparison-arm-receipt",
  "executionId": "<same-executionId>",
  "planDigest": "<same-planDigest>",
  "runId": "<planned-runId>",
  "blockId": "b01",
  "arm": "A",
  "position": 0,
  "failure": null,
  "cleanupErrors": [],
  "armStartedMonotonicNs": "2000000000",
  "workflowStartedMonotonicNs": "4000000000",
  "workflowFinishedMonotonicNs": "6000000000",
  "armFinishedMonotonicNs": "8000000000",
  "rawDirectory": "arms/<planned-runId>/raw",
  "rawLockSha256": "<SHA-256 of original raw.lock.json bytes>",
  "environmentDirectory": "arms/<planned-runId>/environment",
  "environmentPlanSha256": "<SHA-256 of environment/plan.json bytes>"
}
```

`failure` 和 `cleanupErrors` 必填。仅 `failure=null` 且 `cleanupErrors=[]` 允许该臂通过；
例如 postprobe 后 driver 退出异常、OOM、释放超时或清理失败，即使 HTTP raw 已完成也保留 invalid。
控制器必须先完成所有受管进程/容器清理，再以 create-only 方式写最终 receipt。

控制器完成全部臂后，再次核验整个固定运行时身份，写入唯一全局终态 `terminal.json`：

```json
{
  "schemaVersion": 1,
  "kind": "http-comparison-execution-terminal",
  "executionId": "<same-executionId>",
  "planDigest": "<same-planDigest>",
  "runtimeDigest": "<same-runtimeDigest>",
  "status": "completed",
  "failure": null,
  "runtimeReverified": true,
  "finishedMonotonicNs": "9000000000",
  "performanceAccepted": false
}
```

缺少终态、失败终态、复核未完成、结束时间早于任何臂或超总预算，使整组 invalid。
`execution.json`、`terminal.json` 是 execution 根目录仅有的两个内容文件，其他证据归属各计划臂。

`execution.lock.json`：

```json
{
  "schemaVersion": 1,
  "kind": "http-comparison-execution-lock",
  "planDigest": "<same-planDigest>",
  "contentDigest": "<paired/core valueHash(files)>",
  "files": [{"path": "execution.json", "bytes": 1234, "sha256": "<64-hex>"}]
}
```

`files` 列出目录下全部常规文件，排除锁本身，推荐按 path 排序。
拒绝符号链接、特殊文件、`..`、未列入的文件、重复路径、未知 runId、字节/摘要不一致。
适配器在重建后再次核验完整锁，防止读取期间变更。锁不是外部签名；它证明当前快照内部一致性，
不提供第三方可信时间或无法伪造的执行证明。历史手工功能联验缺少上述预注册过程，不能追认成配对实验。

## 验证边界

每臂重新运行 HTTP `analyze()`，验证 suite、题目/负载/顺序、请求和响应摘要、前后检查、trace 预算、
正确性和 phase 内时序。只允许 evidenceFiles 的运行路径变化，附件字节/摘要与冻结列表逐项一致。
指标从重新计算的分析取得；goodput 的 observations 从 measurement trace 中 `outcome=ok` 且
`endMs<=durationMs` 的记录计数，避免用浮点吞吐反推样本数。

环境证据检查精确 attempt、镜像、容器、非 root、安全/资源设置、发行版/JDK、配置及正常停止记录；
前后模式探针重新解析实际 Thread.print 文件，并核对 request 窗口、VM.version、PID/starttime/boot/
namespace/JDK 身份。探针响应原文未留存，因此 nonce 响应的校验仍依赖已固定 probe 实现的记录；
输出明确写出该限制，不把 status 单字段当作模式证明。

全序列按冻结 block/arm 顺序执行、无重叠、无重复原始运行/容器/attempt；先 registration，后 arm。
前探针结束在 workflow 开始之前，后探针开始在 workflow 结束之后。HTTP trace 当前只保存 phase 相对时间，
因此这里验证包含 warmup/measurement/drain 的**完整 workflow 控制器窗口**，不声称独立建立了每个
measurement phase 的绝对单调时间锚点。实际 phase 总时长必须能放入控制器窗口。

所有计划 block、arm、指标都保留。缺失/错误/失败记录会让对应 arm invalid，顺序/整体条件问题让整组
invalid，paired/core 对失效家族不输出可用于结论的区间；不会删掉坏样本提高置信度。
`rawEvidence` 仅代表 HTTP 原始证据已重验，`runtimeArtifacts` 代表已核对的运行记录一致；环境独占、
运行真实性、driver 校准、统计覆盖率和正式验收不会由这些状态自动推导。标准 paired analyzer 单独读取
observations 时仍保留其保守 rawEvidence gate；本适配器生成的 analysis.json 才带有此次独立核验范围。

## Driver 与资源证据复核

M2 的有效臂还必须包含以下原始证据；缺失时保留 invalid，不退回仅检查 HTTP 状态：

```text
arms/<runId>/
  controller/
    driver-before-start.json, driver-started.json, driver-final.json
    driver-ready.json, go.json, workflow-result.json, release.json
    process-identities.json
    collector-roles.json, collector-stop.json, collector-window.json
  collector/
    spec.json, samples.ndjson, summary.json, manifest.json
```

`process-identities.json` 包含 `schemaVersion:1`、`kind:http-comparison-process-identities`、
`hostBootId`、`recordedMonotonicNs`，以及 `roles` 中的 target、driver、driver-supervisor 三条记录。
每条保存 `hostPid`、`statRaw`、`statusRaw`、`cgroupRaw` 和 `namespaces:{pid,net,mnt,user}`。
它们分别指向真实 JVM、driver 的 Docker init 和 driver Python 监督进程。适配器重新解析 PID、
NSpid、启动 ticks、有效 UID/GID 和 cgroup membership；拒绝僵尸/已退出进程及超范围 PID。
它把 target 的容器 PID/namespace 与模式探针关联，把 driver init 与 inspect.State.Pid 关联，
把 driver-supervisor 与 driver-ready 的 `pid/startTimeTicks` 关联。进程原始文件在 collector 启动前采集。

Driver 的三个 inspect 必须指向相同的新容器及固定 runtime/image/runId；逐一核对网络命名空间、
只读挂载、非 root、CPU/内存/PID/swap、cap-drop、no-new-privileges、tmpfs 和精确命令。
完整 workflow 回执须与 arm 回执的单调时间一致；最后必须正常 exit 0 且无 OOM。
墙钟仅用于 Docker 总存活预算的交叉检查，workflow 顺序仍用同一 boot 的 monotonic 时间。

`resource-evidence.mjs` 独立读取 collector manifest 和全部样本，不依赖 summary 的 completed 标志：

- 核对精确文件清单、字节/摘要、spec、固定角色、boot、PID/start ticks、cgroup 路径/device/inode。
- 重建样本和角色的实际时序、相邻计数差、CPU 均值、内存/PID 观测峰值、OOM/PID 失败及缺项摘要。
- 累计计数严格保留 uint64 十进制字符串；计数回退、控制上限变化、伪造区间或摘要均拒绝。
  CPU 上限对照 Docker NanoCpus，允许内核 CFS 配额换算的一微秒量化差。
- 缺失指标仍为 null 并保留具体原因。例如 `io.stat` 不可读会使 coverage=partial，不能补成 0。
  必需计数字段和 PSI 形状对齐 producer；空 CPU/OOM 字段不能伪装成完整覆盖。
- 在读取 chunk 时限制字节预算；读取期间追加且没有换行的内容也不能让 readline 无限缓存。
- 从原始 stop marker 重验 token/spec/boot/结束时刻；首个完整样本必须结束于 workflow 开始前，
  最后完整样本必须开始于 controlled-stop 请求之后。实际角色样本首尾覆盖包含整个 workflow。
- 根控制器的 collector-window/roles 回执必须与重建结果一致。生产配对桥接要求
  `sourceOrigin=linux-procfs`；通用 reader 可以验证 fixture 格式，fixture 无法充当生产臂的主机证据。

最后一次 `/proc` 身份核对由固定 producer 的终态记录证明，当前协议未单独保存那次原始文件。
已记录样本间的 skippedSlots 可精确重建；最后一次写入后的调度跳槽只有终态累计数，适配器验证其
可行上下界并明确这一限制。采集开销、未观测指标、宿主独占、第三方干扰和统计校准仍待验收。

JSON 中的每臂 `resourceSamples/resourceObserved/resourceMissingMetrics` 和离线 `report.html`
展示同一份重建结果。这些资源窗口覆盖整个 workflow（含控制协调、预热、测量与排空），
没有按某个 measurement phase 归因。HTML 与全部 JSON、renderer/core/resource 等实现身份一起
进入输出内容锁；离线分析依赖的 package lock 是依赖声明，不能替代已安装依赖树的真实性证明。
