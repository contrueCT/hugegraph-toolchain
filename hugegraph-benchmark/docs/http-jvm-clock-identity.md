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

# JVM 原始时钟身份

mode probe 在原有检查的前后保存 `result.json.clockIdentity`。mode probe 和 service 只加载相邻固定
`jvm-clock-capture.py`，不生成 bytecode cache；无任意模块或命令入口。该块独立于执行模式的
`identityBefore/identityAfter`；读取失败或已读身份漂移使 clock 状态为 unknown，执行模式仍由原有
sentinel、线程栈和进程检查决定。没有新增 jcmd、采样线程、网络请求或逐请求工作。

```text
startup jvm-runtime.json    mode-pre result.json       HTTP phases       mode-post result.json
  actual JVM + clock       raw before → existing probe → raw after      raw before → probe → raw after
            \________________________ independent raw verifier ________________________/
                                  host domain: candidate
```

`clockIdentity` 的 schemaVersion 为 1、kind 为 `http-jvm-mode-clock-identity`，包含：

- `sourceOrigin`：实际 `/proc` 为 linux-procfs；测试替代路径为 fixture，且不读取宿主身份。
- `captureComponentSha256`：固定 helper 原始 bytes 的 SHA；before/after 各自在 bracket 内只读该
  regular file，限制 1 MiB 并重核元数据。marker 与 snapshot 相核，外部期待值另外提供。
- `before/after`：每次 snapshot 的 `startedMonotonicNs/finishedMonotonicNs` 均为十进制字符串，
  包围该 snapshot 的所有读取及首次 binary hash；其顺序也必须位于 probe 原有开始/结束时间内。
- `capture/jvm`：实际 PID、原始 stat/statAfter、由它们提取的 start ticks、boot ID、
  ns/time 与 ns/time_for_children 原文、timens_offsets 原文、实际 `/proc/.../exe` 路径与文件元数据。
- `capture.python/clock`：实际 Python executable/version/implementation 与 get_clock_info(monotonic)。
  API 固定为 time.monotonic_ns，candidate 仅接受 clock_gettime(CLOCK_MONOTONIC)、monotonic=true、
  adjustable=false。
- `unavailable` 为字段到固定失败码的映射；`changedFields` 从已读稳定字段重建。
  `verified:false/hostAssociation:unverified` 固定；自报 status 或 changedFields 不能跳过独立核验。

每个原始 stat 上限 4096 字节、offset 上限 256 字节、boot 上限 128 字节；offset 必须依次为
monotonic/boottime 两行，各含规范整数秒和 0..999999999 纳秒。Python executable 的首个 hash
限定为 256 MiB、2 秒，文件描述符在读取前后核 regular-file、大小、device/inode、mtime/ctime；
结束仅重核元数据。JVM 不增加 hash，实际 exe 元数据与 startup 的冻结 Java 对象相核对。
整个 clockIdentity JSON 上限 64 KiB。该预算为 clock 块预算，外层 result 文件包含原有模式证据。

原始 stat 的 CPU、RSS、状态等字段可正常变化；稳定身份单独比较。每次 probe 内，Python PID/start
应稳定；pre/post 两个 probe 的 Python PID 可以不同，其 executable 元数据、摘要、版本及实现须一致。
同一 JVM 的 PID/start/boot、实际 exe 元数据和各次 namespace/offset 全部核对。ctime 变化拒绝 candidate。

## 独立核验接口

`jvm-evidence.mjs` 导出纯函数 `verifyJvmClockIdentity(runtime, launch, context)`，主
`verifyJvmEvidence` 将其结果写入 `clockIdentity`。`runtime/launch` 是本次 startup 原件，主 verifier
先按冻结环境计划验证 argv、Java/libjvm 摘要、maps 及原始 stat；clock verifier 另核原始 stat 和时序。

`context.clockExpected` 可带以下独立期待值：

```text
modeProbes: { pre: 完整 mode-pre result 原件, post: 完整 mode-post result 原件 }
modeProbeComponentSha256
captureComponentSha256         // 固定相邻 helper 的外部原始 bytes SHA
containerPythonExecutableSha256
containerPythonVersion          // 与 sys.version 原文精确比较
containerPythonImplementation   // 如 cpython
jdkMonotonicSource: {
  kind: "http-jdk-monotonic-source-expectation",
  fixedSourceRevision: "30ef840c3736270330fc0a26849c2456406facfe",
  sourceDigests: [固定源码摘要...],
  javaExecutableSha256, libjvmSha256,
  clockApi: "CLOCK_MONOTONIC"
}
```

期待值从外部冻结资料提供；不能从待验 receipt 的自报摘要补值后宣称独立来源。JDK source 字段仅记录
`matched-expectation`，单独声明或正确版本字符串均不能证明二进制到源码的等价映射。
本实现不消费调用者自报的 collector verified 布尔位，不提供 proven 状态。

启动时 `jvm-runtime.json.captureIdentity` 新增 schemaVersion:1、kind:
`http-jvm-initial-clock-identity`、sourceOrigin、captureComponentSha256、snapshot，以及同样的
verified/hostAssociation/hash 预算。snapshot 与 mode 的单次读取结构相同，保存实际启动 Python 的
binary hash、clock_info、原始 proc/ns/offset 和 JVM 端点身份；没有新增服务结束采样。
`runtime.clock.observedBeforeMonotonicNs/observedAfterMonotonicNs` 包围 helper 和原有 namespace
读取，`runtime.observedAfterMonotonicNs` 在全部数据读取完成后赋值。读取不可用保留 unknown，
实际 JVM argv、已加载 libjvm 和进程 proof 继续独立验证。

完整新回执另核原始 stat 的 PPID：启动 Java 的实际 Python PID 是其父进程，startup、mode-pre、
mode-post 的 JVM 原始 stat 必须保持该父子关系。只核现有原文，不增加采样。初始完整 raw 与 read
bracket 相核后，可给 initialCapture:raw-consistent、initialClockWindow:bounded；独立运行时来源门仍
保留。旧 startup 缺新字段时继续显示缺口，不能由后续 probe 倒推初始 Python 身份。

原件齐全且内部一致时返回 `rawStatus:raw-consistent`、`hostDomain:candidate` 和规范化 domain。
缺失/不可读取返回 unknown/unverified，缺失字段与自相矛盾的完整原件则拒绝。旧 evidence 没有 clock
块时继续保留既有观测结果，并显式给出 mode-clock-receipts-unavailable。

下列门持续 pending：startup capture Python 的独立运行时来源、独立容器映像/动态运行时绑定、
固定 JDK binary/source 映射、本次独立 collector 域关联、phase attribution、observer overhead。
旧 startup runtime 只有 capture 的 namespace/offset；旧 observedAfterMonotonicNs 早于其实际读取，
initial-clock-read-window 和 initial-capture-runtime-source 继续显示。新 producer 的真实 bracket
通过独立校验后可关闭读取区间缺口，旧原件的状态保留。
整个 HTTP 窗的 loaded libjvm 连续性也需后续实际 maps/来源资料；startup maps 的成功不能替代它。

固定源码链和浮点误差模型见 封存研究（本机记录未随包分发）。后续
phase 映射使用整数 timenanos，不能按 UTC/Unix epoch 解释，不能用 timenanos−uptimenanos 推出精确
JVM 起点。GC 的日志采样时刻和报告 duration 各自保留语义，边界 bracket 内的事件保持不确定。

本地 fixture 覆盖结构/篡改/unknown/预算；Linux 新 attempt 才能验证实际 procfs 与 namespace 收集。
旧冻结快照及 sealed evidence 不会被新 verifier 的离线结果追认为成功；性能接受始终为 false。
