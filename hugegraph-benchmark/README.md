# HG-Bench

HG-Bench 定义可复现的图数据库基准规范与题集；`hgbench` 负责组织执行、收集证据和生成报告。
已有服务可使用[普通 HTTP 入口](docs/http-run.md)，吞吐评测使用[HTTP quick/standard](docs/http-throughput.md)；
大图查询使用[图查询 quick/standard](docs/graph-query.md)。两轨共用调度、执行器和离线 SVG 报告，每张图可独立导出。
HTTP 已有真实 HG 功能与描述性测量证据；图查询五题已通过协议功能链，真实 SF100/HStore 已完成全部顶点内容核验和部分边导入，图查询测量尚未开始。
完整29类官方 driver 适配、离线转换、Loader封装和校验组件已有实现。
规范与工具仍为本地候选，完整SF100、工程校准、独立复现和[阶段一发布门](docs/phase1-acceptance.md)尚未全部通过，未对外发布。

完整范围、关键缺口与验证边界见[阶段一验收对照](docs/phase1-acceptance.md)。

## 安装与命令发现

在本目录使用 Node.js 22.14.0 或以上版本安装和构建 CLI：

```bash
npm ci
npm run build
node dist/cli.js --help
node dist/cli.js http --help
node dist/cli.js schema command:http
node dist/cli.js schema command:http.calibration.acceptance
node dist/cli.js docs list
node dist/cli.js docs read progress
```

`--help` 面向人阅读；`schema command:http` 返回 HTTP 子命令、参数和选项的 JSON。
用点分路径可直接查询单个入口，例如 `schema command:http.calibration.acceptance`，无需读取整个命令树。
这些查询只读取命令定义，不执行目标命令；内部工作进程命令不公开。
`docs list/read`离线读取随包分发的指南；`docs read progress`返回阶段一验收范围及未完成条件，本机运行日志保留在执行工作区。
普通命令默认返回 `{schemaVersion, ok, command, data, nextActions}`，错误也使用 JSON，并配合非零退出码。
从 [HTTP 指南](docs/http-benchmark.md) 选择单次执行，或从 [受控配对执行](docs/http-comparison-run.md)
选择 Docker 环境与 A/B 比较。`plan` 固定输入，`task run` 实际执行，`report` 离线重建；
先查看对应子命令的 `--help`，其中列出副作用、示例和恢复方法。

构建后的这些检查只读取本地输入：

```bash
node dist/cli.js http validate examples/http-gremlin.json
node dist/cli.js http generate examples/http-suite-functional.json \
  --out /absolute/new-profile.json --dry-run
```

已有目标服务和一份 profile 时，可直接完成普通运行与自动报告：

```bash
node dist/cli.js http run /absolute/profile.json --out /absolute/new-run --dry-run
node dist/cli.js http run /absolute/profile.json --out /absolute/new-run
```

前台返回每个单元的原计数、质量门、完成窗吞吐和原 JSON/HTML 路径；异步、查询和取消见[一条命令指南](docs/http-run.md)。

Python、Java、Docker 和目标服务按具体组件需要配置；CLI 安装本身不部署它们。
已有任务按原 `--root` 和 `--request-id` 查询，断线后继续查看原任务；完整操作见 [组件任务](docs/component-tasks.md)。

## HTTP 与图查询 quick/standard

| 测试轨        | 执行入口              | quick                          | standard                           |
| ------------- | --------------------- | ------------------------------ | ---------------------------------- |
| HTTP 服务吞吐 | `http throughput run` | 标量、多行、1MiB响应三道代表题 | 全11题，增加并发档位和独立窗口复测 |
| SF100 图查询  | `graph run`           | 全五题、同一SF100和独立参数池  | 同五题，增加并发档位和独立窗口复测 |

使用各自已准备的profile，先通过`describe`查看协议，或用`--dry-run`只读预览：

```bash
node dist/cli.js http throughput describe
node dist/cli.js http throughput run /absolute/go-profile.json \
  --preset standard --out /absolute/new-http-standard --dry-run
node dist/cli.js graph describe
node dist/cli.js graph run /absolute/graph-profile.json \
  --preset quick --out /absolute/new-graph-quick --dry-run
```

省略`--dry-run`执行对应负载并生成报告。profile准备、完整题义、默认窗口、校准缓存和恢复方法分别见
[HTTP指南](docs/http-throughput.md)与[图查询指南](docs/graph-query.md)。HTTP standard默认整任务上限90分钟；
图quick/standard要求同一SF100源的实际快照达到所选完整度，并完整导入查询依赖的关系；本次选择95%，可配置100%，详见图查询指南。
正式测试需要按指南关闭HG内存监控触发的手动GC及宿主swap，并保留测量窗证据。

旧[`http preset` v0.1](docs/http-presets.md)继续提供三题forward/embedded配对quick，旧standard/full为设计状态。

## 第一阶段：规范与题集

SF100 的[独立规范候选](benchmarks/ldbc-snb-interactive-v1/spec.md)现包含完整 29 类题目契约、
固定版本参考源码、答案校验器、成绩规则、校准设计和发布门槛。draft.3 已加入三个 SF100 资产内容锁和 119 个成员清单。
可直接用 Python 标准库离线校验，
也可经 CLI 发现：

```bash
python3 benchmarks/ldbc-snb-interactive-v1/verify.py verify
node dist/cli.js benchmark spec
node dist/cli.js benchmark questions --id IC14
```

全部 29 类操作已实现，完整官方 driver 已构建并接入 HTTP 适配器。
44 条有序单元记录通过官方 DbValidator 和 HugeGraph memory 后端检查，包含更新回滚；
12 类传输/提交异常检查通过。全分片核对器已接入恢复决策。
补充开发检查已打通真实HugeGraph Gremlin HTTP和语义图计数/schema/索引核对。
完整SF100输入转换及独立Person/knows参考已验收，旧机单机多Store已确认全部顶点内容及20.610%的边记录。
用户已决定换机，旧机自有测试已停止，数据与原始证据保留；图quick/standard、性能校准和独立复现仍待完成。
全部发布条件与未完成范围见[阶段一验收对照](docs/phase1-acceptance.md)。
转换、Loader及原始文本保留见[数据流水线](docs/sf100-data-pipeline.md)，实际入口和边界见[适配器与核对器](docs/ldbc-adapter.md)。

主基准优先接入 [官方 LDBC SNB Interactive v1](benchmarks/ldbc-snb-interactive-v1/README.md)：
固定官方数据、参数、更新流、参考结果与 driver 版本；SF0.1 资产锁定和初始图离线转换保留为历史经验。
正式大图源规模固定为 **SF100**，源图为282,637,871顶点与1,775,513,741边；部分导入快照按实际范围披露，当前等待新机器安排。
当前定位见 [套件注册表](benchmarks/registry.json)。

[Distributed Read](benchmarks/distributed-read-v0.1/spec.md) 调整为面向 HStore 的合成专项诊断集：

- 12 类任务，连通图、普通/高度数顶点、倾斜入流量、全局 ID 参数流和大结果集。
- dev、百万级、千万级、亿级顶点规模档；最大候选档约 12 亿条有向边。
- 流式数据生成、逐文件 SHA-256、192 组校验向量、独立 SQLite 参考校验器。
- 固定的性能协议候选，以及缓存/工作集、分区/副本、跨节点访问、后台活动的披露要求。

可先用 `describe` 查看精确规模；大规模数据需要显式生成。本地生成验证不代表 HStore 性能验证。

[Core Read v0.1 草案](benchmarks/core-read-v0.1/spec.md) 包含：

- 12 类只读任务，数字/字符串 ID 共 36 组参数；两遍执行共 72 次操作。
- 两份确定性小图，每份 110 个顶点、282 条有向边。
- 参考答案、可接受答案约束和独立校验器，覆盖重复、漏结果、错误 ID 类型、分页、路径与完整属性。
- 固定一致性执行规则、成绩卡 schema、完整性与版本锁定文件。

Core Read 是语义检查集。验证它只需 Node.js 22；全套本地检查还需要 Python 3.9+ 的标准库 SQLite。
参考规范校验不用启动数据库，也不依赖 npm 包：

```bash
node benchmarks/core-read-v0.1/reference.mjs verify
```

完整工具测试使用上方安装步骤准备依赖后运行 `npm test`；其中包含实际本地组件进程和 HTTP 夹具。
仓库的 `.github/workflows/benchmark-ci.yml` 复用这些检查，先构建锁定的 HTTP 和全量内容核验 Go 组件，
再显式设置 `HGBENCH_GO_BINARY`、`HGBENCH_SCAN_GO_BINARY` 执行各自的真实 CLI/协议检查，
并运行两个组件的 Go 单元测试和29类题目的规范校验。
未设置对应变量时，相应 Go 检查会明确跳过；组件 CI 与完整SF100验收分别记录。
使用冻结实验草案的专项检查保留在源码仓库的`tests-experiments/`；该目录说明其额外输入和独立运行命令，不随npm包分发。

`tools/conformance.mjs` 提供最小 REST 导入、只读执行和离线成绩复核。
它核对 HTTP 原始结果与答案，拒绝漏题、错题、分页未消费完成及证据篡改；已通过本地传输测试，真实服务适配待验证。
性能规模、参数流和测量协议已形成候选。当前远端实测暂停，后续按新机器安排接续；
HStore 实测校准和独立环境复现仍待完成，尚未产生 HugeGraph 性能成绩。

SF100 运行契约与离线检查见 [运行契约](docs/sf100-contract.md)：

```bash
npm run build
node dist/cli.js benchmark validate benchmarks/ldbc-snb-interactive-v1/sf100-plan.json
node dist/cli.js benchmark plan benchmarks/ldbc-snb-interactive-v1/sf100-plan.json
node dist/cli.js schema sf100-plan
```

组件请求/回执的 `benchmark exchange`、检查点 `benchmark checkpoint` 和恢复建议 `benchmark recover` 见 [文件协议](docs/sf100-protocol.md)。

上述规划命令不下载数据、不执行组件、不写入数据库。`contractValid` 表示配置有效，`executionReady` 表示执行条件；
统一 SF100 执行器尚未实现，规划会列出缺口；独立组件的入口及验收状态见下方文档。
自动化调用可用 `--require-ready` 要求就绪，否则退出 3。

官方数据接入命令和当前覆盖见 [LDBC 接入指南](benchmarks/ldbc-snb-interactive-v1/README.md)。
原有合成/小图命令、产物说明和验证边界见 [本地交付指南](docs/benchmark-local.md)。

分片转换与受控 Loader 入口见 [数据流水线](docs/sf100-data-pipeline.md)。
独立参考库输入桥接见 [参考映射与转换](adapters/ldbc-reference/README.md)。

## 工具与组件任务

已确定 TypeScript CLI 作为组织入口和控制器，通过版本化文件与进程协议调度执行器、分析器。
执行与统计组件可采用适合自身职责的技术栈。CLI 提供 help、schema、JSON 和任务生命周期接口；
报告采用可离线查看的 HTML。

[真实 HTTP 工具链](docs/http-benchmark.md)提供 `http validate/plan/report`，配合 `task run` 执行。CLI 冻结配置与组件身份，独立执行器记录完整响应和答案校验，分析器核对样本并生成交互离线 HTML；当前报告仍为描述性结果。

[新版离线报告与多组对比](docs/http-reports.md)提供 `http compare --input 组=目录` / `--manifest`。
普通报告和多组报告共用 SVG 图表，每张图可独立导出；保留逐轮点、缺项、错误和事件，支持 CSV 与结构化版本身份。

[前瞻配对执行](docs/http-comparison-execution.md)通过 `http comparison plan/prepare` 生成一次性任务，
完成受控 Docker 启停、实际模式核验、持续资源采集及原始证据重建。
[A/A 校准](docs/http-calibration.md)提供多实验计划与汇总；缺失、失败和不足的实验保留为 unknown。
[工程证据汇总](docs/http-engineering-acceptance-report.md)提供 `http calibration acceptance`，从原校准证据生成七项工程门、完整11题覆盖、离线HTML和可锁定的校准索引；缺项和限定范围保持可见。
[工程验收政策](docs/http-engineering-acceptance.md)的v2请求将工程门与可选误报率认证分开；
尚未接入真实校准证据的工程门继续保留unknown。
[吞吐主线](docs/http-capacity-proposal.md)覆盖全11题逐题曲线和独立复测，SLO按需事前配置；
`http capacity` 的规划入口复用现有comparison/task，执行及确认阶段按[实现边界](docs/http-capacity-execution.md)验收。
[分窗诊断](docs/http-phase-windows.md)提供 `http calibration windows`，从原始证据离线重建预热/测量趋势，保留低样本与排空记录；不自动认定稳态。
[驱动自耗](docs/http-driver-self-cost.md)提供 `http calibration driver-cost`，重算记录的校验、解压和 trace 耗时；未测的吞吐收益保持 unknown。
可选 `clockReference` 为实际 runner 的每次 run/probe 采集窗前、窗后原始因果包围，
见[实际 phase 边界](docs/http-direct-phase-clock.md)及[原始 S/R 协议](docs/http-runner-sanity.md)。
这些工具能力分别验收；正式性能仍需通过环境、观测成本、驱动余量和统计校准。

受控比较使用 `http comparison plan/prepare/status/stop/report`，执行继续由 `task run` 管理。
`plan` 固定题目、负载与平衡区组；`prepare` 固定源码、JDK/Node、镜像和预算，生成 `task.json`。
执行器依次创建独立实例、核验前后实际模式、运行负载与持续采集、停止归属容器并封存证据。
断线后查询同一个任务，禁止重复派发已有 attempt；报告保留所有失败或缺失运行。
完整命令与资源设置见 [配对执行](docs/http-comparison-run.md)，输入与解释边界见 [配对证据](docs/http-paired-evidence.md)。
正式校准和性能验收仍待完成，`performanceAccepted` 保持 false。

`task list/plan/run/status/wait/cancel/events/logs/artifacts` 管理真实组件进程，支持后台执行、幂等提交、日志预算和进程组清理；使用方式及边界见 [组件任务](docs/component-tasks.md)。
`components/`、`examples/controller-smoke.json` 及顶层 `run` 命令保留为配对 fixture 控制器原型。
`src/sf100.ts` 和 `benchmark validate/plan` 服务于当前规范阶段。
其中的求和fixture用于调度和协议开发；SF100的统一端到端编排仍待实现。

阶段划分与退出条件见 [实施阶段](docs/stages.md)。

## 源码与本机产物

`evidence/`保存原始执行证据，`deliverables/`保存安装包和报告；两者保留在本机并从Git提交清单排除。
本机进度日志、环境交接和运行诊断也保留在本地，按当前分发规则从新源码快照和npm包中排除；旧快照保持原件。分发文档中的本机记录说明不构成可随包复核的证据。
可复用源码、规范和测试夹具分别位于`src/`、`components/`、`benchmarks/`及`tests-normative/`。
源码检查应能在没有本机原始证据的目录运行；正式数据、参考、JDK/Go和执行预算仍需按相应命令准备。

历史v7所有权回归使用从实际执行源原样提取的三个函数夹具，固定源及函数摘要。
9项边界检查不依赖私有`evidence/`，也不会操作Docker；它们只覆盖该历史实现，当前远端执行另有冻结检查。
需要复核原执行文件时可显式设置`HGBENCH_SUPERVISOR_SOURCE`，校验不会因文件缺失自动跳过。
