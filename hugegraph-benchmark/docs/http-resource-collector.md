# HTTP 连续资源采集组件

`components/http/resource-collector.py` 为 M1/M2 提供独立、只读的 Linux cgroup v2 采集。它保存 driver、target 和所在主机的资源时间序列，供后续质量门与窗口分析使用。组件不生成负载，不调用 Docker、jcmd、JMX，不触发 GC，不启动或停止被测服务。

```text
显式角色/PID/cgroup + 时间/输出预算
                  │
          冻结 spec 与初始身份
                  │
       按固定时刻采样，迟到时跳过槽位
                  │
     身份核对 → 原始计数 → 区间变化 → NDJSON
                  │
         完成 / 取消 / 变化 / 预算耗尽
                  ▼
       summary + 内容摘要 manifest
```

采集开销尚未校准，所有结果固定 `overheadCalibrated:false`、`performanceAccepted:false`。它复用了既有 host probe 的“计数保留十进制字符串、缺失为 null、目录设备号/inode 绑定、计数回退不继续计算”语义，代码独立，不依赖或修改 SF100 工具。

## 入口与严格输入

```bash
python3 components/http/resource-collector.py --help
python3 components/http/resource-collector.py describe --json
python3 components/http/resource-collector.py collect --spec collection.json --out /absolute/new-collection --json
```

`describe` 输出机器可读契约、字段上下限、输入样例、退出码和限制；`collect --help` 提供入口说明。stdout 仅输出一个终态 JSON 对象；不把每次采样打印到 task 日志。

```json
{
    "schemaVersion": 1,
    "kind": "http-resource-collection",
    "intervalMs": 1000,
    "durationMs": 60000,
    "maxSamples": 61,
    "maxOutputBytes": 8388608,
    "filesystemPath": "/var/lib/hgbench",
    "roles": [
        { "role": "target", "hostPid": 1234 },
        { "role": "driver", "cgroupPath": "/sys/fs/cgroup/example-driver", "anchorPid": 5678 }
    ]
}
```

示例 PID/路径必须替换为已核实的真实身份。调用者应在 Docker 环境证据中把容器 ID、`State.Pid`、服务 PID 和采集角色对应起来。该组件验证所选进程与 cgroup 的绑定；它没有 Docker 归属证明，也不证明宿主独占。

每个角色只允许以下一种定位方式：

- `hostPid`：读取该 PID 的 `/proc/<pid>/cgroup`，按当前 cgroup v2 mountinfo 解析目录，绑定 PID 启动 ticks 和目录身份。
- `cgroupPath + anchorPid`：路径是已解析的绝对 cgroup 目录，anchor 必须仍处于这个**确切目录**。同样固定其 PID 启动身份。仅给目录不能判断同目录内的进程重启，因此不接受无 anchor 的路径输入。

采集进程必须看到声明的 host PID 与 cgroup namespace。默认入口只在 Linux 上运行。测试夹具使用专门注入的 procfs 根，结果标记 `sourceOrigin:fixture`，不能作为真实主机证据。

| 字段             | 范围与语义                                                |
| ---------------- | --------------------------------------------------------- |
| `intervalMs`     | 100–60000 ms，固定调度间隔                                |
| `durationMs`     | 100–86400000 ms，且不小于间隔；受控停止模式中表示最大预算 |
| `maxSamples`     | 1–100000 个完整样本；超过即 partial                       |
| `maxOutputBytes` | 128 KiB–64 MiB，覆盖全部四个产物；预留 64 KiB 给摘要/封存 |
| `roles`          | 1–8 个，role 为唯一的短小写名称                           |
| `filesystemPath` | 已存在目录，用于记录该文件系统剩余空间                    |
| `controlledStop` | 可选 `{path, token}`，用于结束实际执行窗口；见下方协议    |

拒绝未知字段、重复 JSON key、非有限 JSON 数值、相对路径和含控制字符的路径。输出目录必须全新，父目录必须已存在。既有目录不会覆盖、续写或重新派发。

## 执行器受控停止协议

固定时长独立采集可以省略 `controlledStop`。HTTP 执行器则可预先冻结以下字段，以实际工作流结束时间收束采集，避免等待最坏执行预算：

```json
"controlledStop": {
  "path": "/absolute/attempt/control/resource-stop.json",
  "token": "0123456789abcdef0123456789abcdef"
}
```

`token` 为每次采集生成的 32 位小写十六进制身份。停止文件位于 collector 输出目录外，父目录须预先存在；组件在启动时解析父目录并拒绝已有停止文件。控制器负责保留该文件，将它纳入外层执行证据锁；collector 不创建、删除或重写它。

控制器先等待 `samples.ndjson` 出现首条完整样本，再释放负载。工作流完成后，以同一主机、同一 boot 的 `time.monotonic_ns()` 生成下列记录。`specSha256` 为 collector 已输出的 **spec.json 原始字节** SHA-256；不要直接摘要控制器原有的不同缩进 JSON。

```json
{
    "schemaVersion": 1,
    "kind": "http-resource-stop",
    "token": "0123456789abcdef0123456789abcdef",
    "specSha256": "<collector spec.json SHA-256>",
    "hostBootId": "<host /proc/sys/kernel/random/boot_id>",
    "workflowFinishedMonotonicNs": "<workflow finished monotonic_ns>",
    "stopRequestedMonotonicNs": "<publication prepared monotonic_ns>"
}
```

纳秒时间必须为规范十进制字符串，并满足 `collectionStart <= workflowFinished <= stopRequested <= observedNow`。禁止使用进程相对的 Node `performance.now()` 直接填充这些字段。控制器须原子、独占发布，例如先在同目录创建临时文件、写完并 fsync，再用 `os.link(temp, final)` 发布并删除临时名；若最终文件已存在则失败，不能覆盖。停止文件最大 4096 字节，字段严格，发布后须保持不变。

collector 读到有效请求后继续原来的固定采样节奏。当一条完整样本的**开始时间不早于 stopRequested**，再核对所有 anchor/cgroup 身份及停止记录，才封存为 `completed`、`reason:controlled-stop`。控制器可以在工作流结束后立即发出请求，collector 自动等待后续采样槽。若工作流期间 anchor 已退出或重启，结束请求不能绕过身份失败。

摘要的 `controlledStop` 保存请求对象、文件字节数和 SHA-256；成功时另有 `coveringSampleIndex` 指向该完整样本。缺少请求、预算内没有完整覆盖样本、请求身份/时间错误或发布后变化都会保持 partial。SIGTERM/SIGINT 是取消，仍然产生 partial；不能用于正常结束。启用此协议后 `durationMs` 仅为最大预算，到期不会自动变成 completed。标记证明控制器声明的窗口边界；完整测量覆盖仍须结合实际 HTTP 工作流回执独立核验。

## 采集内容与时间口径

每条完整样本包含计划时刻、实际开始/结束 `monotonic_ns`、UTC 墙钟标签、采集耗时、迟到量、collector 自身累计 CPU 时间及峰值 RSS。墙钟不参与区间速率计算。各角色另有自身读前/后 monotonic 时间，避免假定多个 cgroup 同时读取。

| 范围        | 原始指标                                                                            |
| ----------- | ----------------------------------------------------------------------------------- |
| cgroup CPU  | `cpu.stat`、`cpu.max`                                                               |
| cgroup 内存 | current、peak、max、swap current/max、`memory.stat`、`memory.events`                |
| cgroup PID  | current、max、events                                                                |
| cgroup I/O  | `io.stat`，按 major:minor 分开保留                                                  |
| cgroup PSI  | CPU、memory、I/O 的 some/full 平均值与累计 total                                    |
| cgroup 状态 | `cgroup.events`                                                                     |
| 主机        | CPU ticks、内存/交换空间、load average、diskstats、CPU/memory/I/O PSI、文件系统空间 |
| JVM         | 明确标记 unavailable；没有 GC/堆/线程/JIT 数据                                      |

累计计数和纳秒时间使用十进制字符串，避免 JSON/JavaScript 的整数精度丢失。缺失、无权限、格式损坏或读取超限的指标为 `null`，同时保存原因；缺失不会变成 0。`memory.stat` 包含 gauge 与累计值，保留原文键语义，不盲目对全部字段作差。I/O 按设备保留，禁止把 mapper 与底层物理设备相加当成总 I/O。

相邻完整样本计算 cgroup CPU、memory/pids events、I/O 和 PSI total 的区间变化。CPU 平均核数为 `usage_usec` 差值除以该角色的实际观测间隔。指标或设备在任一端缺失时，对应差值仍为 `null`。摘要提供观测到的最大 memory.current/PID、CPU 有效区间覆盖与均值、OOM kill/PID 分配失败计数；`memory.peak` 单独标记为最后一次读取的累计峰值，它可能早于本次采集窗口。

采样慢于预定时刻时跳过过期槽位，记录 `skippedSlots`，不会补发一串密集样本。采集组件应在预热/测量前启动，并在测量与排空后留出至少一个间隔；分析器必须依据**实际观测时间**确认窗口覆盖，不得把声明的 duration 当成首尾都有样本的证明。

## 身份、失败与封存

初始身份绑定 boot ID、PID namespace、anchor PID/start ticks、cgroup 路径、mount ID、设备号/inode 与类型。每次角色读取前后重新核对，结束时再核对 anchor/cgroup。PID 重用、退出、迁移、cgroup 替换、控制上限改变或累计计数回退都会停止当前 attempt，不拼接到新实例上。稳定 anchor 之外的进程可以变化；若要检测 JVM 自身重启，调用者须选择该 JVM 的 host PID 为 anchor，不能用容器 PID1 代替这个保证。

`summary.clockIdentity` 另外保存 collector 自身在采样前、后的原始时钟身份，作为未来关联 HTTP phase 的输入：

- 实际 Python `/proc/self/exe` 的 SHA-256、字节数、设备/inode、mtime/ctime 纳秒元数据，
  以及报告的解释器路径、版本、实现名称。仅在采样起点前计算一次 SHA，结束时复核元数据。
- 自身 PID、原始 `/proc/self/stat`、启动 ticks、boot ID、time/time_for_children namespace、
  原始与解析后的 `timens_offsets`，以及 `get_clock_info('monotonic')` 原始声明。
- 每次身份采集的开始/结束 monotonic 时间、读取不可用原因、已观测的变化字段。

实际二进制摘要最多读取 256 MiB，并在每个 1 MiB 块的边界检查 2 秒协作式截止；
初始摘要在采样开始前完成，不占用声明的采样时长。结束只复核身份和文件元数据，每条资源样本没有新增文件摘要。
摘要记录 `digestRead`，结束快照的 `sha256=null` 明确表示没有重新哈希。单次系统调用阻塞仍由外部 task/cgroup 限制。

`availability=observed` 只表示这些自身字段成功读取且前后一致。来源、平台、权限或某个读取不可用时，
保留 `availability=unknown` 与原因；原有资源采集可继续。fixture 不读取测试宿主的实际 Python/proc 身份，
始终显式 unknown。身份、已读取文件的元数据发生变化或已知字段在结束时不可读，会封存为 partial，
保留已有完整样本和原始 before/after 现场；首次哈希期间已检测到文件或进程漂移时不开始采样。

所有路径保持 `verified=false`、`hostAssociation=unverified`。本块未核实加载的动态库来源，
未证明与 Node/JVM 使用相同 time namespace，也没有实际 runner 的 S/R sanity 证据；
缺项不能凭 Python 版本、1 ns 分辨率声明或历史独立 peer 结果补成已关联。

SIGTERM/SIGINT 只设置无锁取消标志，循环在短等待与读取边界检查，然后保存 partial。没有子进程或后台守护进程。采集截止检查覆盖样本边界和角色边界；单个内核/文件系统读取以及最终身份/封存 I/O 可能延长退出时间。外层通用 task/cgroup 必须提供硬运行、CPU、内存限制，不能依赖 Python 解决内核不可中断 I/O。

| 文件             | 内容                                                                       |
| ---------------- | -------------------------------------------------------------------------- |
| `spec.json`      | 规范化后的确切输入                                                         |
| `samples.ndjson` | 完整原始样本；预算允许时还保留带 rejection 的被拒样本                      |
| `summary.json`   | 完整/partial 终态、原因、身份、缺失覆盖、时间/计数摘要、资源观测与组件 SHA |
| `manifest.json`  | 最后写入的封存记录，含前三个文件的大小与 SHA-256                           |

`http-resource-rejected-sample` 不进入有效样本数和资源均值；其用途是保留计数回退等错误现场。输出预算不足时不会再塞入被拒样本，摘要明确记录 `rejectedObservationPreserved:false`。

**封存以 manifest 存在且全部文件大小/摘要核验通过为准**。SIGKILL、宿主重启或磁盘写入失败可能只留下部分文件；即使 summary 已写入，也不能绕过 manifest 验证。未封存目录保留为 interrupted/unknown，只允许新 attempt。`status:completed` 仅说明完成采集时段；缺项、采集成本、窗口覆盖和基准有效性仍由后续质量门判断。

退出 0：完成采集。退出 2：partial、取消、身份变化或运行/输出预算失败。退出 64：开始采集前的输入/环境不满足。所有路径都不自动重试服务或切换目标。

## 与通用 task 组合

用 `hgbench task plan` 冻结 `python3`、本组件和 collection spec，再由 `task run` 执行。组件继承 task 进程组，不 daemonize。task 的超时应包含采集时长及有限封存宽限，stdout/stderr 日志预算单独声明；原始 NDJSON 只写组件输出目录。`task cancel` 的 TERM 会产生可封存的 partial，后续 KILL 仍可能留下 unsealed 产物。

当前测试覆盖夹具中的真实文件读取、身份替换/退出/迁移、counter reset、控制变化、缺失和损坏指标、特殊文件拒绝、采样/字节预算、迟到跳槽、受控停止的身份/时间/后续样本覆盖与变更拒绝，以及真实子进程 CLI 在已发布停止请求时的 SIGTERM partial 封存。真实 Linux/Docker cgroup v2 采集、与 HTTP 执行窗口关联及采集开销校准尚待主流程完成。

自身时钟增量测试另覆盖 fixture 不读取实际宿主、摘要字节/截止预算与 FIFO 拒绝、结束不重哈希、
实际临时文件变更/丢失、PID/namespace/offset/时钟声明漂移转 partial，以及初始已知漂移在首样本前拒绝。
这些本地测试不替代 Linux/Docker 实际身份联验或宿主关联校准。
