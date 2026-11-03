# HTTP 驱动记录的自身耗时

`components/http/driver-self-cost.mjs` 离线重算已有 raw 的校验、解压和 trace 生产耗时。
先完整调用 `analyzer.mjs` 校验 raw lock、题目答案、附件、计数、时序、服务检查及预算，
再按原始锁重验实际读取的 run/trace 摘要、字节数和 phase 计数。损坏证据拒绝，不生成可用报告。
`quality.verdict/gates` 与每 run 的 `qualityIssues` 保留原值，`performanceAccepted=false`、
`overheadAccepted=false` 始终不变。

```bash
hgbench http calibration driver-cost --help
hgbench http calibration driver-cost /absolute/raw-directory \
  --out /absolute/new-driver-self-cost.json

# 独立组件入口
node components/http/driver-self-cost.mjs --help
node components/http/driver-self-cost.mjs \
  --input /absolute/raw-directory \
  --output /absolute/new-driver-self-cost.json
```

输入目录需要完整 `raw.lock.json`；输出是一个新 JSON 文件，父目录需存在，已有文件直接拒绝。
成功标准输出只返回 kind、raw 摘要、run 数和 false 标记；错误使用 `http-component-error`
JSON 与退出码 `6`。不需要在线 HugeGraph、Docker、Python 或网络。
公共 TS CLI 只监督固定组件，返回标准 JSON envelope、报告绝对路径及 read next action；
组件失败映射为 `HTTP_CALIBRATION_FAILED`，输出上限16MiB、截止330s，另有最多5s组清理。

模块接口：`analyzeDriverSelfCost(rawDirectory)` 返回对象；
`reportDriverSelfCost(rawDirectory, newOutputFile)` 校验后以 `wx` 写入 JSON。
结果不包含分析时间或本机输入/输出路径，可在移动完整 raw 后逐字节重建。

## 计算口径

每个 workload run 的 warmup、measurement 分别输出：

- 全记录、已发送、未发送、正确、发送后错误、完整/不完整响应，以及两类错误响应计数。
- `validation.recordedTotalMs/recordedCompleteResponses/meanRecordedMs`：所有完整响应中记录的
  `validationMs` 总量/样本数/均值，包含错误答案、HTTP 错误等完整错误响应。
- `decode` 使用同样结构，只包含实际非 null 的 `decodeMs`。
  解压耗时已经包含在 validation 中，两者不能相加。
- `unavailableOrNotRecorded` 区分未发送、不完整响应、完整响应但未记录此项。
  未记录 decode 不能自动视为 0；没有样本时总量/均值为 null。

完整响应上的数值仍须通过 analyzer 的非负/时间边界校验。
合法记录为 0 会保留 0；缺旧 `evidenceWriteMs` 显示 unavailable，非法负值、null 或字符串拒绝。

trace 生产只提供 run 级结果：

```text
meanRecordedMsPerTraceRecord
    = run.evidenceWriteMs / (warmup记录数 + measurement记录数)
```

分母包含所有实际记录，包括未发送、drop、取消前未发送和错误。
不使用仅 measurement、仅成功或仅已发送请求作为分母。`traceBytes` 也由实际文件重验。
原始 run 没有逐 phase 写耗时，所以不产生逐 phase 写盘成本。
preflight/postcheck 仍由 analyzer 验证，其耗时不混入 workload run 的总量或分母。

## 有界异步 trace 写入

新runner使用`trace-writer.mjs`：序列化后进入最多1MiB的内存队列，包含当前写入中的完整记录，
通过异步文件API按原记录顺序处理短写。队列超限、写失败、零长度写或最终sync失败都会使run失败；
不等待磁盘来节流到达、不丢弃trace来维持成功，也不修改负载参数。成功返回前等待全部字节写完并sync。
该改变将磁盘写等待移出Node调度线程；JSON序列化、响应解压与答案校验仍在主线程。

新run的`traceWriter.mode=async-bounded-v1`单独记录：

- `serializationAndEnqueueMs`：主线程JSON序列化、队列入账墙钟。
- `asynchronousWriteWaitMs`：各次异步write开始到完成的累计墙钟，包含等待。
- `finalFlushWaitMs`和`finalSyncMs`：测量phase结束后的队列排空等待、最终sync墙钟。
- 队列预算/峰值、记录数、入队/落盘字节和写调用次数；成功回执必须字节及记录守恒。

这些时间可能重叠，不能相加当作CPU成本或测量延迟。异步run不再输出旧`evidenceWriteMs`；
self-cost将旧同步成本显示为不可用，原因是`async-writer-costs-reported-separately`，
新字段在`traceProduction.asynchronousWriter`内按原口径展示。旧run继续按旧同步规则读取，原证据不重解释。

变更依据见004的请求/资源离线诊断：慢臂同步trace累计墙钟与同workflow I/O PSI同时上升。
这支持修复驱动写路径，尚未证明唯一磁盘根因或异步方案实测收益。新runner来源变更需要新的真实验证，
不能沿用004或旧runner校准作为通过证明。普通文件I/O没有内核级硬截止，外部task/container监督预算继续适用。

## 能说明什么

这些值是驱动记录的 `performance.now()` wall duration，不是隔离后的 CPU 时间。
旧同步run的`evidenceWriteMs`覆盖旧runner `emit`内的JSON序列化和同步trace write调用；
最终 fsync、open/close、其他证据写入、page cache 和后台写回成本没有被完整覆盖。
均值不代表移除校验/trace 后会获得相同幅度的吞吐或资源收益。

raw 不保存响应 body，无法重新执行原始校验或解压。此组件只重算已有时长与计数，
不做伪 replay，不生成“关闭 validator/trace 后”的对照，也不发布因果成本或性能验收。
当前 headroom v1 的观测栈及 task 查询方式限制见 `http-driver-headroom-plan.md`；
已有 v1/004 原件保持不变。

## 上限与验证

原始输入继续受 analyzer 的全工作流 trace 256MiB、run 数5120、单行64KiB等预算约束；
本 consumer 的 run JSON 上限8MiB、单 trace 上限256MiB，输出 JSON 上限16MiB。
raw manifest 在首次摘要前通过同一 FD 的 regular/非 symlink/≤8MiB 检查，再有界读取原字节；
超大 manifest 会在摘要或完整 analyzer 读取前拒绝，后续 manifest 重读沿用同一上限。
先完整计算并检查输出预算，再创建文件；使用期间输入需保持不变。
独立组件没有目标调用、响应重放或额外子进程；公共 CLI 为该离线组件提供上述有界监督。

专项测试覆盖精确合成计数/全部记录写耗分母、完整错误与无响应、合法零/缺值、坏锁及重封坏数据、
不覆盖和移动后字节重建，并通过真实本地 gzip HTTP 的正确/错误/超时 raw 独立复算总量。
合成样本只证明协议，真实本地 HTTP 只证明工具路径；没有以此声称校准已通过。
