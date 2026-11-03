# HTTP quick / standard 吞吐入口 v0.2

状态：首版实现已完成，本机与 Linux Docker 的协议联验、真实 HG 默认 quick 及短窗口 standard 功能链已验证。固定交付包的默认 standard 已在三个 HG HTTP 栈上完整运行，每组 132 个测量单元；本轮使用私有 tmpfs、8 GiB 驱动及显式 90 分钟预算。该记录仍为描述性观察，性能资格须通过[阶段一验收](phase1-acceptance.md)的相应门；默认时限见[整任务预算](http-total-deadline.md)。两档均使用固定并发闭环：请求完成后立即补发，
主指标是测量窗内正确完成的请求数/秒。旧 `http preset` v0.1 保留原定义。
本入口组织已有 HTTP 任务、Go/Node runner、独立 raw reader 与 SVG 报告；不部署服务。

## 使用

已有服务及生成的 HTTP Serving profile 时：

```sh
hgbench http throughput describe
hgbench http throughput run go-profile.json --preset quick --out ./quick-001 --dry-run
hgbench http throughput run go-profile.json --preset quick --out ./quick-001
hgbench http throughput run go-profile.json --preset standard --out ./standard-001
```

profile 沿用现有 `http generate` / `http engine register` 入口，保存目标、编码、执行器和独立答案。
新版调度覆盖该 profile 的旧负载、时间与重复设置，具体覆盖值在 dry-run 和新 `plan.json` 中展示。
quick 选择三道代表题，standard 覆盖全部 11 题；原题目与答案字节不变。
较低可压缩响应的补充题属于后续题集版本，本增量尚未加入。

| 参数                       | quick                                 | standard                       |
| -------------------------- | ------------------------------------- | ------------------------------ |
| 题目                       | scalar-add / rows-4096 / string-1024k | 原 11 题                       |
| 并发探索                   | 1 / 4 / 16                            | 1 / 2 / 4 / 8 / 16 / 32        |
| 每探索点预热 / 测量        | 2s / 8s                               | 5s / 10s                       |
| 复测点                     | 每题候选点                            | 每题候选及一个邻近点，最多两点 |
| 每复测点预热 / 测量 / 轮数 | 5s / 30s / 1                          | 5s / 30s / 3                   |
| 固定窗口上限               | 195s，3分15秒                         | 3300s，55分钟                  |

这张表仅适用于 HTTP 线，与图查询默认时间分别定义。固定窗口不含首次校准的 80s，
任务准备、服务前后检查、排空、分析及收尾另计。时长为配置推导，实际耗时看 run 状态。
默认复用现有服务，复测使用新时间窗与新 driver 进程；没有自动重启或独立 JVM 复现声明。

`--settings overrides.json` 可覆盖 `describe` 中的设置。自定义窗口明确记录为
`customSettings=true`；联验时使用短窗口不能当作完成默认标准档测量。
并发必须严格递增；流程不在预算不足时自行减少题目、轮数或窗口。

## 校准与复用

首次执行启动已有固定正确响应 provider，使用两个代表检查：

- scalar-add：1000 rps，64 个在途上限，10s 预热 + 30s 测量。
- rows-4096：40 rps，64 个在途上限，10s 预热 + 30s 测量。

provider 使用预先构造且经独立参考验证的响应，校准期间不调用 HG。
保留 provider 计数/资源、driver 原始请求与完整答案校验；启动前的首个资源样本单独保留，
不用于稳态 provider 余量判断。校准仅覆盖所记录的 loopback 路径、结构、大小和请求率。
大字符串、不同网络路径、超出速率及后续图查询语义校验都不能自动继承资格。

缓存默认位于 `<root>/calibration/`，可用 `--cache` 指定。缓存绑定执行器、源码、主机资源、
编码、连接/超时、完整校验方式和题集摘要。复用时重新读取并独立验证 raw 与 provider 摘要。
`passed` 与 `limited` 的完整诊断记录均可复用，复用后保留原状态和限制；
部分执行、损坏原件或不匹配记录不会被复用。这样相同环境的已知问题不会导致每次自动重跑。

需要新的记录时显式运行：

```sh
hgbench http throughput calibrate go-profile.json --out ./calibration-002 --refresh-calibration
```

缓存引用原始证据目录，不能只保留缓存 JSON 后删除原件。跨目录迁移时应一起保存原件，
路径失效后将作为 cache miss，旧记录不会被改判。

## 选点与质量边界

共用调度器按预声明阶梯执行，选择正确吞吐距离最好观察值不超过 5% 的最低并发，
在候选和下一邻近档（顶档则取前档）使用新时间窗复测。5% 为工程启发式，可配置。
最高档仍增长或只有边界点时报告平台未建立；未找到候选时保留未完成项。

每个实际点保留完整答案、错误/超时、驱动 CPU/节流/内存、请求和 trace 预算状态。
有错误、漏发、驱动压力或资源耗尽的点不参与候选选择。资源隔离未证明作为 unknown 保留，
其吞吐只能称为观察值。复测按并发给出各轮值、中位数与范围；至少满足配置轮数才汇总。
P99 样本不足时继续显示 unknown。报告不提供跨题总分、业务 SLO 容量或统计显著性保证。

排空及在途数来自客户端请求记录。请求超时、取消后，目标是否仍在执行查询须由目标侧证明；
客户端在途数归零不能证明目标空闲，也不能据此排除对后续窗口的影响。预热中的请求错误同样使该点失去候选资格。

当前便利入口尚未自动关联目标 JVM / GC 与宿主 swap 的逐窗证据，报告明确显示环境 unknown，
`performanceAccepted=false`。正式性能使用仍须关闭 HG 内存监控触发的手动 GC（或记录获准高阈值）、
验证 swap 关闭，并补齐目标与测量窗口关联。现有受控环境/比较工具继续可用。
请求和答案校验成功属于工具功能验收，不能据此宣布数据库容量或完整阶段一验收完成。

## 预算、取消和恢复

`--budget-ms` 默认覆盖本次运行，`--budget-mode total` 为默认值；`stage-sum` 要求覆盖预览中的
保守阶段预留。未自定义 settings 的 HTTP standard 默认总上限为 5,400,000 ms（90 分钟）；
固定预热/测量窗口仍为 55 分钟。显式 `--budget-ms` 优先，quick、图测试和校准专用模式沿用各自推导。
自定义 settings 沿用窗口推导预算，dry-run 展示 `fixedWindowMs`、`defaultBudgetMs`、实际 `budgetMs`。
请求执行、校准、准备和 reader 共享剩余时间，不自动续跑。
到时记录 partial / 未完成项，保留清理时间；若报告未能在剩余预算内完成，写入 `reportFailure`，
之后可离线重建。硬上限依赖操作系统正常终止和文件系统响应，超时回执不等于清理证明。

默认每阶段最多 100 万请求、每个 child 最多 1GiB trace、整次最多 8GiB trace，磁盘剩余
低于 2GiB 时停止。均可在 settings 中配置。命中上限不参与饱和判断，原计数与预算理由保留。
旧 profile 的请求数和 trace 取值没有自动更改。

入口为前台编排，SIGINT / SIGTERM 会取消同一个 active child 并收尾。
`state.json` 保存 task ID、已完成阶段与未完成题目；原始 child 使用现有 task 生命周期。
若 CLI 被强杀或 SSH 断开，先检查原状态及原 task，必要时取消该 task；不删除原目录重试。
provider 在控制进程消失后收到 EOF 即退出，另有时间上限。首版不支持整流程 detach 或自动恢复。

```sh
hgbench --root ./state task status --id task-...
hgbench --root ./state task cancel --id task-...
hgbench http throughput report ./standard-001 --out ./standard-rebuilt
```

离线报告逐份重验测量 raw，并复核阶段配置、选点与复测身份。原目录可迁移后重建，无需修改冻结计划。校准栏展示运行/缓存复用时核验的记录，不代表离线报告重新授予校准资格。缺失或损坏数据保留为 partial。
复用现有 HTML / JSON / CSV 和逐项 SVG；新增并发—正确吞吐曲线，曲线及每个指标图可独立导出。

2026-10-28 反馈修正：Go 失败包装的根错误和未完成运行身份保留在 partial 中，嵌套 result 不进入有效统计。
确认值显示实际窗口数；默认三轮共享同一服务 JVM，独立 JVM 重复、平衡候选顺序和 A/A 资格需外部实验。
校准 provider 的新 summary 保留最多 16 种错误来源/code/次数/首末单调时刻，计数单位为错误处理器事件，
不能直接当作失败请求数。旧错误计数与 limited 判定保持；请求原文、认证头和 rawPacket 不写入诊断。

离线 `http throughput report` 可选 `--target-diagnostics /observations/manifest.json`，
将已有 GC 日志及 swap 采样按近似墙钟关联到固定阶段窗口，并归档原始附件；
用法、内容锁及限制与[图查询的离线诊断](graph-query.md#可选的已有-gc--swap-日志诊断)共用。
它不启动采集、不排除慢点，也不补授正式逐窗环境资格。
