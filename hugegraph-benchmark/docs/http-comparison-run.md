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

# HTTP 配对执行器

`components/http/comparison-workflow.py` 将前瞻配对计划接到真实单机 Linux Docker 生命周期。
它支持 A/A 和 forward/embedded，按照冻结的 block/arm 顺序各执行一次。源码、构建、JDK、Node、
镜像、外围配置、driver 资源和采集参数在首次派发前绑定；只有 mode 随计划改变。
当前用途为 functional/calibration，所有出口保留 `performanceAccepted:false`。

```text
comparison plan + runtime settings
           │ freeze：内容/身份锁，不启动容器
           ▼
       新 attempt ── run 一次 ── execution 注册所有 runId
                                   │
                    每臂按冻结顺序，禁止并发/重试
                                   ▼
 prepare → fresh target/task → ready → pre mode probe → settle
                                   │
             fresh driver Docker（同 target network）
                                   │
      collector 首完整样本 → go → HTTP plan/task/workflow
                                   │
     workflow done → stop marker → collector 后覆盖样本/封存
                                   │
         post mode probe → driver release → 独立 stop/清理
                                   │
              小证据归档 → arm receipt → 下一臂
                                   │
           全部身份复核 → terminal + execution lock
                                   ▼
                    paired-evidence 离线重验
```

## 入口

公共 CLI 提供计划、prepare、status、stop 与离线 report；实际执行交给通用 task，支持断线后查询同一次任务。

```sh
node dist/cli.js http comparison --help
node dist/cli.js http comparison describe --execution
node dist/cli.js http comparison prepare /absolute/runtime-settings.json \
  --plan /absolute/frozen-comparison --out /absolute/new-attempt
node dist/cli.js --root /absolute/tasks task run \
  --plan /absolute/new-attempt/task.json --request-id comparison-001 --detach
node dist/cli.js --root /absolute/tasks task status --request-id comparison-001
node dist/cli.js http comparison status /absolute/new-attempt
node dist/cli.js http comparison stop /absolute/new-attempt
node dist/cli.js http comparison report /absolute/new-attempt/execution \
  --plan /absolute/new-attempt/comparison --out /absolute/new-report
```

`prepare` 自动生成 `task.source.json` 和内容锁定的 `task.json`，返回 taskPlan/taskPlanDigest 与下一步命令。
它只读取本地资产并写新目录，不拉镜像、不发 HTTP、不启动 Java。任务使用冻结的绝对 Python 路径和
attempt 中的组件，固定 `effect:"database"`、30 秒终止宽限及 1 MiB 日志上限。外层 timeout 为 comparison
总预算 + stopMs + sealAndAnalyzeMs + 10 秒；若派生值超过通用 task 的七天上限，prepare 在冻结资产前
明确拒绝，需调整显式计划预算。task 跟踪组件、runtime/lock、comparison plan/lock；完整资产由组件复核。

执行要求本机 Linux、非 root UID/GID、现有 Docker Unix socket、cgroup v2、可读宿主 `/proc`，以及已存在
的固定镜像。镜像必须含给定 Python 路径和 HugeGraph/JDK 动态库。`report` 只读密封证据并生成新的 JSON
验证、分析和内容锁；退出 0 时仍须检查 `valid`、`statisticsEligible` 与具体 issues。缺臂报告可以成功生成，
同时保持 valid=false；已有输出目录不会覆盖。

需要直接调用组件时仍可使用以下入口。通常由 prepare 生成的 task 负责调用 run，无需手写 task source。

```sh
python3 components/http/comparison-workflow.py --help
python3 components/http/comparison-workflow.py describe --json
python3 components/http/comparison-workflow.py freeze \
  --runtime /absolute/runtime-settings.json --plan /absolute/frozen-comparison --out /absolute/new-attempt
python3 /absolute/new-attempt/comparison-workflow.py status --attempt /absolute/new-attempt
python3 /absolute/new-attempt/comparison-workflow.py stop --attempt /absolute/new-attempt
```

断线后检查同一个 request-id/attempt。`dispatch.json` 使用独占创建，任何结果都保留它；再次 `run`
拒绝派发。`status` 返回记录状态，`dispatch-recorded` 不代表进程仍然存活。需进一步检查通用 task
及当前 environment 状态。`stop` 原子发布持久取消标记，并逐个核对已派发 driver/target 的身份后停止。
Docker create 暂未产生 ID 属于创建窗口，不会触发补跑。新的实验必须有新的 comparison id 和 attempt。

## 冻结输入

以下示例数值只展示资源约束，正式负载/时长仍需校准。时限须根据 comparison plan 中的推导预算配置。
路径在 freeze 时转为真实绝对路径；Docker executable 的文件名为 `docker`。

```json
{
    "schemaVersion": 1,
    "kind": "http-comparison-runtime",
    "sourceRoot": "/absolute/hugegraph-benchmark",
    "nodeHome": "/absolute/node-v22.14.0-linux-x64",
    "hostPython": "/usr/bin/python3",
    "dockerExecutable": "/usr/bin/docker",
    "driver": {
        "image": "sha256:REPLACE_WITH_64_LOWERCASE_HEX",
        "python": "/usr/local/bin/python3",
        "cpus": 1,
        "memoryBytes": 805306368,
        "pids": 128,
        "tmpfsBytes": 67108864,
        "wallSeconds": 1200,
        "shutdownSeconds": 5,
        "maxLogBytes": 1048576
    },
    "collector": {
        "intervalMs": 1000,
        "durationMs": 900000,
        "maxSamples": 901,
        "maxOutputBytes": 8388608
    },
    "supervisor": {
        "pollMs": 1000,
        "commandMs": 45000,
        "maxCommandBytes": 2097152,
        "minFreeBytes": 85899345920,
        "maxWorkBytes": 4294967296
    }
}
```

字段严格校验，未知字段拒绝。`describe` 列出必需字段与退出码。约束如下：

- target 采用 comparison 中 `inputs/settings.json` 的完整设置，每臂只加入 mode。发行版必须与
  `inputs/build.json` 的完整文件清单一致，JDK 和 distribution 同时加入 runtime 身份锁。
- driver 与 target 使用同一固定 image ID，`--pull=never`。REST endpoint 固定为
  `http://127.0.0.1:18080/gremlin`。此版本仅支持无认证受控环境；选择 gzip 的题集 profile 可适配
  当前 HugeGraph 响应压缩行为。不得把认证值写入设置、argv 或证据。
- `sourceRoot` 锁定 `components/`、`dist/`、`src/`、`benchmarks/`、`node_modules/`、package.json 和
  package-lock.json 的完整清单、内容、执行位及内部相对链接；Node/JDK/distribution 锁定完整树。
  Python/Docker 客户端锁定实际可执行文件。变更资产或实现后重新 freeze。
- `collector.durationMs` 是最大预算，须覆盖 `workflowMaxMilliseconds + 3 × intervalMs`。
  正常结束通过受控 stop marker，不等待最大时长耗尽。`maxSamples` 须覆盖全部预定采样槽。
- driver TTL 须覆盖采集最大预算和 30 秒余量；target TTL 须覆盖启动、两次 probe、settle、采集及余量。
  mode probe 预算为 10–180 秒。CPU、内存、PID、tmpfs、日志、输出和磁盘 reserve 分开声明。
- freeze 合计检查 HTTP trace、附件、采集器、runtime 元数据和命令日志预算，归档逐项扣减剩余空间。
  目录看护是采样保护，仍可能有一个检查间隔的增长；bind mount 没有磁盘硬配额。
- 可选 `settings.jvmObservation` 使用 [固定 JVM 观测 spec](http-jvm-observation.md)。原始 stdout、
  parser 输出上限和 2 MiB 收据余量在 freeze 时加入每臂预算；`maxRawBytes` 必须等于共享日志上限，
  parser 截止加 1 秒须小于封存窗口。停止后归档原日志与实际身份/capture，再由冻结 parser 离线生成
  `jvm/`。partial 观测保留原样，独立桥接重解析后展示缺项，不会成为已校准 GC/稳态证明。
  最终封存窗口还须覆盖 `armCount × (parser.timeoutMs + 5000)`、统计分析上限和 60 秒核验余量。
  全部 arm 预算之外另预留最终封存/分析窗口。HTTP raw 优先保存；JVM 归档/解析失败不跳过其余可用证据。

活跃 attempt 和 driver evidence 的空间看护采用独立的有界采样：每秒至多启动一次，单次最多扫描
100,000 个条目、64 层目录、5 秒，并同时服从当前执行的取消和截止时间。扫描使用不跟随链接的目录
描述符，绑定根目录 device/inode；只允许已枚举的子条目在读取前消失，保存其计数和路径样本。
权限错误、其它 I/O 错误、链接、特殊文件、目录身份改变或超限均使本次执行失败。文件系统剩余空间
单独检查，不依赖枚举到的逻辑字节。扫描期间新建、删除或重命名的文件可能造成采样漏项或重复计数；
该数值只用于运行保护，不用作精确磁盘用量或性能指标。最终归档和内容锁仍使用严格、完整的文件校验。

runtime.json 保存上述输入、planDigest/planContentDigest、完整身份清单和组件 SHA。
runtime.lock.json 绑定 runtime.json 的原始字节；execution.runtimeDigest 使用 runtime.lock.json 的
**文件 SHA-256**。每臂归档相同两文件，使离线桥接能复核与实际 HTTP/Node 实现身份的关联。
这些内容锁没有外部签名，不提供不可伪造的执行证明。

## 生命周期与证据

driver 使用非 root、只读根文件系统、drop ALL capabilities、no-new-privileges、内存与 swap 相等、
固定 CPU/PID/tmpfs、禁止自动重启和 Docker 日志。source、Node、comparison、control 只读；只有
`/evidence` 可写。容器不挂 Docker socket，不开放宿主端口，网络加入精确已核验 target 容器。

宿主控制器、target wrapper、driver supervisor 分别具有时限；SSH 消失不会解除容器 TTL。
SIGTERM/SIGINT 只设置无锁标志，命令按短周期检查；命令超时清理整个归属进程组，包括 leader
已退出但子进程还持有管道的情况。driver 调用公共 HTTP plan/task/artifacts，失败时取消相同 task key；
容器 PID1/init 退出是其内部残留进程的最终边界。collector 有自己的固定最大 duration，首样本前失败
先 TERM 并给有限封存宽限，超限才 KILL。任一 cleanup 失败不会跳过另一个容器的 cleanup。

停止窗口由 `stopMs` 统一约束。窗口耗尽后仍为另一个 owner 保留最多一秒的应急命令尝试，保存超预算
失败；target/driver 各自 TTL 继续有效。封存使用 `sealAndAnalyzeMs` 和剩余字节，超时、强杀、未停或
证据不完整都使该臂 invalid。不可中断内核 I/O 仍需要宿主恢复机制；组件不承诺内核挂死时即时停止。

collector 开始前保存 target JVM、driver init 和 driver Python 的宿主 PID、原始 proc stat/status/cgroup
及 namespace 链接。target JVM 通过相同 PID namespace、NSpid 和 start ticks 映射到 ready 中的 Java PID。
等待首完整样本后才发布 go；完成 HTTP workflow 后发布包含精确 spec SHA、bootId 和 monotonic 时间的
stop marker。collector 采到 marker 之后的完整样本、验证身份并封存后才运行 post probe。
整个 workflow 的时间来自同一 Linux boot 的 Python monotonic clock；HTTP phase 另保存
[原始起止时间区间](http-direct-phase-clock.md)，包含实际 drain。其身份和区间会离线重验，宿主关联
当前保持 unverified；Node `performance.now()` 与宿主时钟没有直接相减的入口。

```text
attempt/
  runtime.json, runtime.lock.json, comparison-workflow.py
  comparison/                 冻结计划
  dispatch.json, current.json, stop-request.json?, result.json
  work/<runId>/               原执行目录和空图运行数据，失败现场保留
  execution/
    execution.json            首次派发前注册全部 runId
    terminal.json             全局执行/运行时复核终态
    execution.lock.json       全快照内容锁
    arms/<runId>/
      receipt.json            时序、原始锁、失败和清理错误
      raw/                    完整 HTTP workflow output
      environment/            计划、配置、生命周期、pre/post probe；排除 RocksDB/JDK/dist
      collector/              spec、samples、summary、manifest
      jvm/                    可选：固定 parser 输出、原日志副本、内容清单
      controller/             runtime 锁、driver inspect、proc 映射、stop marker、命令摘要
  analysis/                   paired-evidence 重建输出
```

每臂 `controller/host-active-usage.json` 保存截至该臂结束的宿主 attempt 累计采样；
`controller/driver-active-usage.json` 保存该 driver 的采样。记录包括预算、首次/末次样本、观察到的
峰值、消失条目总数和至多 8 条路径样本。路径相对各自采样根目录，使用 `unicode-escape` 表示且
每条最多 256 个字符，截断会显式标注。失败样本保留错误码、有限路径和可用 errno。这些小型证据在
正常结束或异常收束时写入并进入内容锁；强杀或宿主失效可能使最后尚在内存的采样无法保存。

成功停止的 target/driver 容器按精确身份移除。工作目录保留，包括空图数据；归档不复制这些数据。
清理历史 attempt 应在核验小证据后按用户磁盘策略单独执行，组件不会删除未知目录或失败现场。

所有命令保存 exitCode/stdout/stderr，CLI 的 stdout 结构化错误不会丢失。缺失臂、错误臂、顺序异常和
清理错误保留在原位置；没有补样、换 runId 或删掉坏臂。最终结果区分：

| 字段                                    | 含义                                         |
| --------------------------------------- | -------------------------------------------- |
| executionCompleted                      | 全部臂及收尾身份复核完成                     |
| evidenceValid                           | 离线桥接复核通过                             |
| statisticsEligible                      | 观察数量等统计资格满足；功能配置可能为 false |
| performanceAccepted / inferenceAccepted | 本组件始终 false                             |

退出 0 仍须按这些字段判断结果。`run` 的 `ok` 要求 executionCompleted 与 evidenceValid 同时为 true；
bridge 失败、超预算或 invalid 不会被标成成功。身份变化写入密封的 terminal，避免仅落在锁外状态中。

## 验证范围

本地测试覆盖严格输入、FIFO/链接拒绝、内容锁与不可覆盖 marker、实际子进程超时/信号/继承管道清理、
创建中状态、逐臂顺序、唯一派发、pre probe 失败、独立清理、受控 driver argv、外国容器拒绝、资源
manifest/窗口、预注册、bridge 失败传播及全局身份漂移。状态机使用明确的本地夹具，不证明真实 Docker
执行性能。前置 environment/probe/HTTP 组件已有独立真实联验；本执行器的完整配对 Docker 联验需由
主流程使用冻结的新 attempt 完成。A/A、采集开销、driver headroom、已知回归、饱和曲线和正式性能验收
保持待完成。
