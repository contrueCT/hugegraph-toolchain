# 主机与 cgroup 观测

`tools/sf100-host-probe.py` 是独立的只读 Linux 组件，兼容直接 SSH 调用和 `hgbench task` 调度。它读取资源现状和 1–10 秒被动计数器差值，不产生压测负载。

```bash
python3 tools/sf100-host-probe.py --path /data --sample-seconds 3 \
  --cgroup /sys/fs/cgroup/<owned-unit>
```

在 Linux 执行主机上，也可用统一任务入口运行：

```bash
node dist/cli.js task plan examples/host-observation-task.json --out host-task.json
node dist/cli.js task run --plan host-task.json --detach --request-id host-001
node dist/cli.js task status --request-id host-001
```

将实际数据路径和 cgroup 路径填入源计划的 command 数组，再冻结计划。`--cgroup` 可重复使用，单次最多 16 个；通过 systemd `ControlGroup` 属性定位所属组，不能根据进程名推测归属。
完整观测保存在该任务的 `stdout.log`，生命周期证据由 `task artifacts` 验证。数据较长时，`task logs` 只显示尾部并标记截断，不适合将截断内容当作完整 JSON。

当前输出为 schemaVersion 3，保留主机、文件系统、CPU、I/O 和 cgroup 字段，新增 PID 分配失败计数与 resourceSignals。历史 schemaVersion 1/2 观测保持原样。

| 观测 | 解释 |
| --- | --- |
| memory.current / memory.stat | 区分整个 cgroup 计账、匿名内存、文件缓存和内核计账；接近 MemoryMax 不等同于 JVM 堆耗尽 |
| pids.events / resourceSignals | 区分历史 PID 分配失败、窗口内新增失败和端点是否已达 PID 上限；缺失为 null，即使差值为零也保留当前已满信号 |
| memory.events | 明确区分触及上限、OOM 和 OOM kill；输出窗口内差值 |
| cpu.max / cpu.stat | 保存配额、周期、CPU 时间、限流计数与时间；平均 CPU 核数来自 usage_usec 差值 |
| memory.peak / swap / pids | 采集原始内核值；平台缺项或读取失败保留 null 和 unavailable 字段 |
| 两次 cgroup 身份 | 记录路径、设备和 inode；中途消失、更换或计数器回退时，不合并两次计数 |

字节数和累计计数使用十进制字符串，避免 JS 对大整数截断；`max` 表示没有该项 cgroup 上限。CPU 平均占用等派生速率使用数值。
`memory.stat` 同时包含字节和计数器，字段名 `memoryStatBytesAndCounters` 保留这一区别。

`interval.valid` 只说明所观测计数器具备同组、无回退的差值条件；缺失项仍为 null。窗口内配额改变会标记 controlsChanged。
主机与各 cgroup 读取并非一个原子快照，采集本身也有成本；该组件不能证明物理机独占、环境稳定或性能结果有效。
正式 Collector 还需持续时间线、JVM/Server/PD/Store 指标、采集成本校准和缺项质量门，本增量只提供可复用的被动观测组件。

`resourceSignals.failureEventsWithinInterval` 只有在同一 cgroup、时间区间有效且对应计数可读时才提供差值；计数器重置时保留 null。`cumulativeFailureEventsAtEnd` 保留截至终点的累计值，不能把历史事件归因于本窗口。`pidsAtLimitBefore/After` 独立反映端点 PID 耗尽，即使短窗口内没有新增请求也不会被零增量掩盖。内存 max 事件只代表触及上限，不归为 OOM。
这些信号可以进入实验质量门；全部为零仍不能独立证明服务健康或成绩有效。
