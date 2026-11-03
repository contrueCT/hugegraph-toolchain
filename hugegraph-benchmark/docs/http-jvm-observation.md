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

# JVM GC/JIT 观测组件

`components/http/jvm-observation.py` 提供固定启动日志预设及有界离线解析。当前只创建计划和解析已关闭的
日志，不启动 JVM、不 attach、不执行 Docker，也不判断 warmup 是否稳定。环境组件已接入启动/捕获，
独立 verifier 重放原始日志并核实际进程/JDK 身份；旧冻结版本和原始验收状态保留。

```text
显式 spec → plan + 内容锁 → supervisor 后续接线
                                  │
           固定 server JVM argv → 同步 stdout → 有硬字节上限的 server.log
                                  │
               正常停止 + 排空到 EOF + launch/capture 回执
                                  ▼
       parse → 原始日志副本 + events + uptime windows + summary + manifest
```

## 启动预设与空间边界

唯一预设 `hotspot17-gc-jit-v1` 生成一个 argv 元素：

```text
-Xlog:gc*=info,safepoint=info,jit+compilation=debug:stdout:timenanos,uptimenanos,pid,tid,level,tags
```

它仅加入 server 的启动参数，InitStore 和版本检查单独保留。禁止运行期间改日志级别和
`-Xlog:async`。JDK 默认同步写日志；异步日志的中间缓冲耗尽时会丢弃消息。原生 `filecount/filesize`
支持轮转，但 filesize 是近似目标。[JDK 17 java 文档](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html#enable-logging-with-the-jvm-unified-logging-framework)

源码在写入并 flush 后判断轮转，还会覆盖下一个旧归档。因此 `(filecount + 1) × filesize` 不能作为
严格磁盘上限，也不能保证全程日志仍在。本组件选择 `rotation:none/maxFiles:1`，复用 supervisor 的
stdout 捕获：每次写入不超过剩余预算，触及上限后停止服务并使观测 partial，保留截断证据。
[固定 JDK 17.0.17+10 轮转实现](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/logging/logFileOutput.cpp#L275)

`maxRawBytes` 必须纳入既有 supervisor `maxLogBytes` 及 arm 证据预算；该上限包含混合 stdout/stderr
中的应用日志。磁盘 reserve 另行检查。同步写日志的扰动仍须用独立校准实验量化。

## CLI 与冻结 spec

```sh
python3 components/http/jvm-observation.py --help
python3 components/http/jvm-observation.py --json describe
python3 components/http/jvm-observation.py --json plan --spec observation.json --out NEW_PLAN
python3 components/http/jvm-observation.py --json parse --plan NEW_PLAN \
  --raw server.log --launch launch.json --capture capture.json --out NEW_REPORT
```

下面的数值是接口示例，不代表校准后参数。字段严格校验，不能传任意 JVM 参数或手工 alignment 状态。

```json
{
    "schemaVersion": 1,
    "kind": "http-jvm-observation-spec",
    "preset": "hotspot17-gc-jit-v1",
    "capture": "supervisor-stdout",
    "maxRawBytes": 16777216,
    "windowMs": 1000,
    "parser": {
        "maxLineBytes": 65536,
        "maxEvents": 500000,
        "maxWindows": 10000,
        "maxOutputBytes": 67108864,
        "timeoutMs": 30000
    }
}
```

plan 写入 `spec.json`、`plan.json`、`plan.lock.json`。锁绑定原始文件字节；plan 同时绑定组件 SHA，
改实现后需生成新计划。raw 上限为 256 MiB，单行上限 64 KiB，事件上限 1,000,000，窗口上限 10,000，
输出上限 512 MiB，解析最长 300 秒。输出预算包含原日志副本、事件、窗口和元数据，必须至少比 raw
上限大 2 MiB；仍可能因事件展开耗尽输出预算，此时保留 partial。读取拒绝链接、FIFO 和特殊文件，
输出目录须不存在。

## supervisor 回执契约

launch 记录启动时的实际 argv、容器内 Java PID、start ticks、boot ID 和冻结 JDK 身份。以下字段必需：

```json
{
    "schemaVersion": 1,
    "kind": "http-jvm-observation-launch",
    "planSha256": "64 lowercase hex: plan.json raw bytes",
    "argv": ["/opt/jdk/bin/java", "EXACT_PRESET_ARGUMENT", "OTHER_ACTUAL_ARGUMENTS"],
    "process": {
        "pid": 123,
        "startTimeTicks": "456",
        "hostBootId": "12345678-1234-1234-1234-123456789abc"
    },
    "jdk": {
        "javaSha256": "64 lowercase hex",
        "libjvmSha256": "64 lowercase hex",
        "version": "17.0.17+10"
    }
}
```

示例占位符必须换成真实值。argv 中唯一 `-Xlog` 选项必须是计划生成的固定值。实际已加载 libjvm 的
归属需由外层核对 `/proc/<pid>/maps`；parser 只复核提供的回执和原始日志，不能证明现场身份。
现有环境的完整 JDK 内容锁、进程映射、启动 argv 及 runtime 锁仍需一起归档。

capture 在日志关闭后写入，必须明确是否读到 EOF、是否碰到字节上限、是否使用强杀：

```json
{
    "schemaVersion": 1,
    "kind": "http-jvm-observation-capture",
    "planSha256": "64 lowercase hex: plan.json raw bytes",
    "launchSha256": "64 lowercase hex: launch.json raw bytes",
    "raw": { "bytes": 12345, "sha256": "64 lowercase hex: server.log raw bytes" },
    "startedMonotonicNs": "100",
    "finishedMonotonicNs": "200",
    "readBytes": 12345,
    "retainedBytes": 12345,
    "discardedBytes": 0,
    "eof": true,
    "capHit": false,
    "forcedKill": false,
    "cleanupComplete": true,
    "error": null
}
```

捕获时间来自 supervisor，当前只检查其先后关系。它们不用于转换 JVM 时钟。连接前需要环境组件
在 TERM/kill 收束期间继续有界排空管道，最终 EOF 后关闭并生成 capture；单纯关闭 selector 不能
证明尾部完整。日志、launch、capture 和启动配置在同一环境证据中封存，不用事后补写成功布尔值。
`readBytes = retainedBytes + discardedBytes` 且 `raw.bytes = retainedBytes` 必须成立；短写、EIO、
丢弃字节或 cleanup 未完成即使已读到 EOF 也保持 partial。error 为 null 或最多 500 字符的捕获错误，
cleanupComplete 取实际 termination 回执。parser 根据原始字段计算完整性，不接收 `complete:true`。

## 解析语义

- 原始 `server.log` 保留；每条事件附原字节 offset/length、timenanos、uptimenanos、PID/TID、级别和
  tags。多线程日志允许时间顺序交错，窗口按 JVM uptime 分桶，不依赖文件行顺序。
- 数字 decorator 接受 HotSpot 的 ASCII 右侧空格 padding，例如 `[233 ]`。日志输出为每个 decorator
  保留此前最大宽度，短 TID 因而可能补空格。数字仍须是规范十进制；PID/TID 的取值、进程绑定、
  最大宽度和原始字节边界继续严格检查，左侧空格、tab、数字内空格及超宽 padding 使解析 partial。
  [固定版本输出格式](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/logging/logFileStreamOutput.cpp#L56)、
  [数字装饰值](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/logging/logDecorations.cpp#L141)
- 只把 `gc` 的 Pause 完成行计入暂停报告总量，按 GC ID/title 匹配 `gc,start`，避免子阶段重复累加。
  缺失、重复、反向或未结束的配对使观测 partial。日志中的 MiB 堆大小只保存为近似值。
  [GC 日志源码](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/gc/shared/gcTraceTime.cpp#L32)
- `jit+compilation=debug` 的普通行记录编译入口活动；另存 `made not entrant`、`made zombie` 和
  `NO CODE INSTALLED`。编译入口日志发生在调用编译器前，不能据此计算成功编译数、编译耗时或 CPU。
  [日志标签](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/compiler/compileTask.cpp#L440)、
  [编译入口](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/compiler/compileBroker.cpp#L2038)
- `n` 标记的 native wrapper 单独记为 `jit-native-wrapper`。固定源码使用 `(native)` 后三个空格，
  再输出空消息或 `(static)`；tiered level 为 0，关闭 tiered 时无 level。它们不计入 `jit-begin`，
  也不产生成功编译数、编译耗时或 CPU。未知后缀、错误标记位置和不支持的 level 保留 unknown/partial。
  [属性及消息格式](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/compiler/compileTask.cpp#L257)、
  [native wrapper 输出位置](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/runtime/sharedRuntime.cpp#L3102)
- safepoint 保存 JVM 报告的各段及总耗时，与 GC 总量分别呈现，避免重叠相加。
  [safepoint 源码](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/runtime/safepoint.cpp#L1053)
- 未识别的相关格式、身份或摘要不符、截断、缺 EOF、强杀、取消和预算耗尽都保留 partial。
  空日志不会被解释成没有 GC/JIT；`warmupStable`、`compilationSuccesses`、`compilationCpuNs` 始终 null。

`windows.ndjson` 仅含观察到日志的 uptime 窗口。没有填零窗口，也不以事件减少证明稳态；暂停报告
总量按完成日志所在窗口计入，不能当作该窗口内的精确暂停占比。一般诊断行仍作为 diagnostic 保存，
原消息可用 offset 从 raw 找回。未知关键语法的原行也可检查。

## 时钟及证据边界

JDK 17.0.17+10 的 timenanos 来自 `os::javaTimeNanos()`；Linux 实现使用 CLOCK_MONOTONIC。
uptimenanos 来自 JVM elapsedTime，两者在构造日志装饰时依次采样，不能用差值伪称精确 JVM 起点。
[两时钟装饰](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/share/logging/logDecorations.cpp#L48)、
[Linux clock 来源](https://github.com/openjdk/jdk17u/blob/30ef840c3736270330fc0a26849c2456406facfe/src/hotspot/os/posix/os_posix.cpp#L1504)

本组件固定 `clockAlignment:"unverified"`、`phaseAttribution:"unavailable"`。现有 mode-pre/post 新增
测量窗外的原始时钟身份，独立 verifier 将 startup、pre、post 的实际 JVM、boot、time namespace/
offset 绑定为 candidate。完整字段、固定读取预算及待验项见 [JVM 时钟身份](http-jvm-clock-identity.md)。
固定 Linux 源码支持直接使用整数 `timenanos`，无需新增时钟 jcmd/attach；实际二进制到固定源码的映射、
宿主域关联与精度门仍需独立验证。日志时间是发出事件的时间，跨 phase 的暂停保留边界不确定性。

```text
NEW_REPORT/
  spec.json, plan.json, plan.lock.json
  launch.json, capture.json
  server.log             完整原日志，或预算/故障中止时的明确前缀
  events.ndjson          有界结构化事件
  windows.ndjson         原 JVM uptime 域窗口
  summary.json           完整性、缺项、计数；不作稳态或性能接受
  manifest.json          所有上述产物的字节与 SHA
```

所有输出目录独占创建。CLI 返回 JSON，退出 0 表示计划生成或完整观测解析，2 表示 partial，3 表示
输入/文件错误，130/143 表示取消。manifest 本身的存在不代表完整；必须核所有内容摘要和 summary。
该内容锁可重建，不构成外部签名或独立执行证明。

本地测试只验证有界解析、固定参数、故障收据及语义，不等于真实 JDK/Docker 联验。

格式兼容修复的固定 tag/commit、主源码摘录及摘要、真实 v4 完整日志的只读扫描另存于
JDK 格式研究（本机记录未随包分发）。该扫描使用新 decoder，逐份核对
原 capture 的 raw 字节数和 SHA，保留新旧 parser/plan 身份。旧 v4 的 failed/partial 结果保持原样；
本组件绑定计划内的实现 SHA，不能直接拿新实现重解析旧计划并追认为旧 attempt 成功。新执行需冻结新计划。

离线独立验证器 `jvm-evidence.mjs` 使用经过内容核对的本地 parser，在临时目录重新解析并逐成员比较。
第一次实际启动 parser 前才安装进程级 SIGTERM/SIGINT 监听；无活跃解析时不保留监听。
并发验证共享一套监听，各 owner 持有自身临时目录直到清理完成。收到信号后停止创建 parser，
全部活跃 parser 进程组先 TERM，最多等待 1 秒再 KILL，并等待子进程 close；重复信号不延长宽限。
验证 promise 拒绝并删除临时输入后，最后一个 owner 移除本模块监听。若原信号由外部 handler
处理过，或清理结束时仍有对应的外部监听，验证器保留应用的信号控制；其余情况重新投递首次信号，
恢复通常的信号退出状态。外层终止 bridge 时应给这段回收及文件清理留出至少数秒宽限。
SIGKILL 和进程/文件系统无法响应的情形仍由外层任务生命周期处理。
