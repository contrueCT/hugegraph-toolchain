# 真实 HTTP 工具链

已有 profile 的普通运行优先使用 [`http run`](http-run.md)，一条命令完成原计划、执行和报告。以下保留可单独审阅的高级步骤。

TypeScript CLI 验证并冻结 profile，通用 task 管理后台进程；`components/http/` 中独立 Node 程序负责 HTTP 发生、分析和离线报告。协议与当前题集见 [HTTP Serving v0.1](../benchmarks/http-serving-v0.1/spec.md)。

标准题集现在可通过 `http suite` 发现，包含 11 个固定题目及独立答案摘要，覆盖结构化多行、1 KiB–1 MiB 单值与 Unicode/转义。用 `http generate examples/http-suite-functional.json --out <new-profile.json> [--dry-run]` 生成 profile；所有时间/负载/预算显式给定，示例只用于功能验证。`schema http-suite-settings` 可发现设置契约。生成的 profile 绑定版本、完整/子集覆盖和内容锁；plan 与原始证据保留整个小型规范包，离线报告再次核对绑定。正式参数和成绩仍待校准。

`http suite --require-release` 在正式条件未完成时退出 3；未知题目/损坏锁/改变已绑定请求退出 2；生成文件已存在退出 4，原文件保留。dry-run 只返回 JSON 与下一步写入命令，不访问目标或创建文件。

新生成的标准题集 profile 启用 `serviceChecks=true`：每题在整轮测量前后各发送一次顺序请求，采用相同完整答案校验、timeout、响应和 trace 预算。前检失败不开始测量，后检失败保留测量与检查文件，均不生成成功清单。检查不建立预热稳定性或实际运行模式证明。自定义 profile 默认关闭；显式开启会增加请求，写入工作负载需考虑这些额外效果。

前后检位于 `output/preflight/` 和 `postcheck/`，raw 协议 v2 将其纳入内容锁；离线分析再次验证每题仅有一条正确响应、摘要/计数及文件身份。每个测量组合另输出实际编码计数和 wire/decoded body 大小分布，混合编码标记为质量问题。旧 raw v1 仍可读取，缺少前后检时不补造记录。

```bash
npm run build
node dist/cli.js http --help
node dist/cli.js schema http-profile
node dist/cli.js http validate examples/http-gremlin.json
node dist/cli.js http plan examples/http-gremlin.json --out /absolute/new-attempt
node dist/cli.js --root /absolute/tasks task run \
  --plan /absolute/new-attempt/task.json --detach --request-id http-001
node dist/cli.js --root /absolute/tasks task status --request-id http-001
node dist/cli.js --root /absolute/tasks task logs --id <task-id-from-status>
node dist/cli.js http report /absolute/new-attempt/output --out /absolute/rebuilt-report
```

使用前填写 endpoint、target 的构建/执行模式/环境说明及 evidenceFiles。当前示例只测算术、结构化结果和 16 KiB 字符串；不修改图。自定义 body 可以修改目标，因此生成的 task effect=database，进程完成后的 effect 仍为 unknown。通用 task 不自动重试。

plan 不访问目标，输出目录必须不存在。plan 冻结组件、Node 可执行文件、profile 和最多 32 MiB 的环境证据摘要；运行时复制环境证据并再次校验。evidenceFiles 由操作者指定，避免包含凭据。描述字段与文件身份属于已记录的声明，需结合部署证据和实际窗口确认目标情况。

`task.json` 是通用冻结任务入口。相同 --root/request-id/计划只返回已有执行；不同 request-id 仍不可复用已执行的 output 目录。断线后先查原任务，新的测量使用新的 plan 目录。task 超时覆盖各场景运行与排空，并额外预留 60 秒组件开销；CPU、内存与磁盘配额交给外部容器/cgroup。

每个 profile 最长约一小时，单 phase 最多 100,000 个请求，响应压缩前后分别最多 4 MiB，全流程 trace 最多 256 MiB，重复请求元数据最多约 64 MiB（含 case 重复部分），输入证据最多 32 MiB。磁盘实际保护还应由部署预算提供。达到 trace 上限保留部分文件并失败，不生成成功清单。任务中断不会自动生成可接受的完整报告。

产物：

- `output/profile.json`、`target-evidence.json`、`target/`：冻结工作负载与目标证据。
- `output/requests/`、`runs/`、`traces/`：各组合的请求协议、运行摘要和逐请求记录；不保存凭据和整个响应正文。
- `output/implementation.json`：组件摘要、Node/主机身份。完整响应 body SHA 与选定 JSON 字段的规范化摘要用于追溯校验。
- `output/raw.lock.json`：原始文件大小/SHA 清单。分析器拒绝丢失/损坏/不一致样本。
- `output/analysis/report.json`、`report.html`：机器可读报告和无需网络的交互 HTML，提供场景/负载筛选、p99 排序及图表。
- `output/completed.json`：工作流完成及原始清单/报告摘要。它不代表性能有效。

`http report` 仅读取原始证据，重建到新目录，不发请求，也不改原始报告。报告中的质量门会保留驱动、环境、A/A、可比性待校准状态。这里的单 profile 流程提供描述性报告。受控 A/B 切换、配对区间和持续资源采集由独立的 [`http comparison` 流程](http-comparison-run.md)提供；真实 A/A、驱动余量与观测成本校准的当前状态见[阶段一验收边界](phase1-acceptance.md)。正式性能和容量结论仍待相应验收。
