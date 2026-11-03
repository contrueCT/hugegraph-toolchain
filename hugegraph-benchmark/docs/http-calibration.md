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

# HTTP A/A 校准计划与证据

本增量实现 `repeatability-aa` 的严格计划、独立证据重算和跨实验汇总。它复用已有 A/A comparison，
每个 experiment 的 A/B 都使用同一 build、mode、题集、负载、外围设置及观测配置。所有物理运行
按原 comparison 协议新建 JVM。当前只接受 `purpose=calibration`；没有默认实验参数、自动重跑或
性能放行开关。正式 forward/embedded 的 mode 单变量规则保持原样。

```text
完整 AA comparison 请求模板 + 预声明 experiment IDs/seeds + 评估 policy/预算
                    |
          calibration plan / verify
                    |
    experiments/<id>/ 既有不可变 comparison 子计划
                    |
      calibration prepare → 一个 task 顺序调度所有 comparison 子 task
                    |
    execution-set.json 显式映射每份已封存原始 execution
                    |
    report → 每份原始 bridge 独立重算 → 单实验效应 / 等价 / unknown
                    |
       跨实验 family 误判计数、二项区间、完整分母与未满足项
```

单个 A/A experiment 可以提供重复性和效应区间；误判概率需要多次独立 experiment 的决策结果。
数学测试、HTTP 功能联验、少量请求、缺失区组或退化区间均不能当作噪声校准通过。

## 命令与 API

先构建 TypeScript CLI；组件的规划和报告代码独立于 CLI。

```sh
hgbench http calibration --help
hgbench http calibration describe
hgbench http calibration plan calibration-request.json --http-plan FROZEN_HTTP --out NEW_CALIBRATION
hgbench http calibration verify NEW_CALIBRATION
hgbench http calibration report execution-set.json --plan NEW_CALIBRATION --out NEW_REPORT

node components/http/calibration-plan.mjs describe
node components/http/calibration-plan.mjs plan --input calibration-request.json --http-plan FROZEN_HTTP --out NEW_CALIBRATION
node components/http/calibration-plan.mjs verify --plan NEW_CALIBRATION
node components/http/calibration-evidence.mjs report --plan NEW_CALIBRATION --input execution-set.json --out NEW_REPORT
```

组件 `describe` 输出原始 capability JSON；其他成功动作输出 `{ok:true,data}`。错误输出 stderr JSON，
退出 6；已有输出绝不覆盖。报告退出 0 可包含 `evidenceValid=false` 或全部 experiment unknown。
公共 CLI 包装为统一的 `schemaVersion/ok/command/data/nextActions`，计划后给绝对路径恢复命令。

JavaScript 导出：

```js
freezeCalibration({ inputFile, httpPlanDirectory, outputDirectory });
verifyCalibration(directory);
reportCalibration({ planDirectory, inputFile, outputDirectory });
```

上述操作只读本地输入和生成新文件；report 会执行本地有界 Node/Python 证据分析子进程。
它们不访问 HTTP、不操作 Docker、不启动目标 JVM。下面的 cohort prepare 将所有子任务编排为一个
既有 `task`，由用户显式执行，遵守同一 request/attempt 的恢复及不重复提交规则。

## 严格请求

`http-calibration-request` 必须完整提供以下字段；额外字段、自由参数覆写及其他 scenario 拒绝。

| 字段                  | 类型与含义                                                                                               |
| --------------------- | -------------------------------------------------------------------------------------------------------- |
| `schemaVersion/kind`  | `1` / `http-calibration-request`                                                                         |
| `id/purpose/scenario` | 有界 ID / `calibration` / `repeatability-aa`                                                             |
| `comparison`          | [原有 comparison 请求](http-comparison-execution.md) 的完整对象；必须为 `aa`、`calibration`，两臂同 mode |
| `experiments`         | 1–100 个 `{id,orderSeed,resampleSeed}`；ID 唯一，均为预声明项                                            |
| `assessment`          | `{confidence,maximumFamilyFalsePositiveRate,minimumExperiments}`，均显式提供                             |
| `budgets`             | `{maxPlanBytes,maxReportBytes,maxReportMilliseconds}`，均显式提供                                        |

每个 experiment 只替换 comparison 的 `id`、`orderSeed`、`analysis.resampleSeed`，保留其余参数。
共同身份文件相对 calibration 请求文件解析；冻结后复制进子计划，不再依赖原目录。
模板自身的 `id/orderSeed/resampleSeed` 仍需满足原 comparison schema。profile 的 benchmark purpose
也必须为 calibration。每个物理 run ID 随派生的子计划绑定，不能跨 experiment 共用运行。

`confidence` 为 0.8–0.999999 的二项区间置信度，误判率界为 (0, 0.5)，minimumExperiments 为
1–10000。允许计划 experiment 数低于声明 minimum，此时报告明确 insufficient-evidence。
这些值只是显式 schema 范围；本组件不提供正式实验阈值或推荐默认值。

计划字节上限为 1–512 MiB，报告字节上限最多 512 MiB、至少 66 MiB，声明总分析时间为
1000–86400000 ms；还必须满足以下保守时间预留：

```text
maxReportMilliseconds >= N × max(60000, child.sealAndAnalyzeMs) + 60000
```

冻结后还读取每份 child 的锁定 settings 和 plan，推导
`reportMaxMilliseconds=max(60000,sealAndAnalyzeMs,parser重算总时限+analysis.maxMilliseconds+60000)`。
启用 JVM 时，parser 重算总时限为 `armCount × (parser.timeoutMs+5000)`；未启用为 0。
父预算必须覆盖全部 child 的该项之和再加 60000 ms。不能用尚未通过实际 workflow 冻结检查的
过小 seal 预算跳过这项验证。

JVM-enabled child 的 seal 预算仍须通过既有 workflow 的逐臂 parser 重算、统计与封存预留检查。
一个 child 分析最多输出既有 adapter 的 64 MiB；每次执行前检查报告仍有 66 MiB 空间余量。
剩余不足时保留该 experiment unknown，不替换计划和分母。计划及报告的总文件清单也受字节上限
检查。普通本地文件 I/O 仍受外层任务/进程监督；内核不可中断 I/O 沿用现有系统边界。

计划输出包含原始 `request.json`、生成输入 `requests/<id>.json`、完整
`experiments/<id>/` comparison、`plan.json`、`implementation.json` 和内容锁。
verify 重新推导父/子计划及当前组件摘要；单纯更新文件摘要不能放行改过的 mode、scenario 或设置。

## 原始 execution 映射

映射文件严格形态为：

```json
{
    "schemaVersion": 1,
    "kind": "http-calibration-execution-set",
    "planDigest": "REPLACE_WITH_FROZEN_CALIBRATION_PLAN_DIGEST",
    "experiments": [
        { "id": "experiment-1", "executionDirectory": "/absolute/sealed/execution-1" },
        { "id": "experiment-2", "executionDirectory": "/absolute/sealed/execution-2" }
    ]
}
```

路径也可相对映射文件。缺少计划中的 experiment 会形成 missing/unknown；未知或重复映射 ID 拒绝。
同一 execution 的 realpath 别名、重复 executionId、重复 runId 或重复 execution lock 都使相关项
无效。按预声明 experiment 顺序，上一份 terminal 时间必须不晚于下一份注册时间，且 boot 相同。
真实原始 bridge 还会重新校验每份 execution 的注册、顺序、实际模式、生命周期、HTTP/raw、资源、
JVM 和各内容锁。目录/ID/时间检查可以发现复用与重叠，不能独立证明随机噪声没有自相关。

report 在 `experiments/<id>/` 保存既有原始 adapter 重算结果和离线 HTML；每份 `rebuild/<id>-command.json`
回执包含本次重建 argv、计时、退出状态和输出。结论保留 execution-set 的原始稳定路径引用，实际
realpath 只用于内部读取/别名检查。`evidence.lock.json` 覆盖确定性的输入快照和派生结果；
本机路径/计时相关的重建审计使用独立 `rebuild.lock.json`，绑定结果 contentDigest，保留审计又不改变
可重复结果摘要。使用相对引用、保持输入原字节及完整目录关系，并沿用相同实现、运行时和分析参数时，
搬移输入树可以保持summary和结果锁逐字节一致；重建审计描述本次操作。上方绝对路径示例若在迁移后
需要修改映射，execution-set摘要及结果中的路径引用也会变化，须保留原映射并说明这些差异，不能宣称
结果锁逐字节一致。运行时或实现改变引起的元数据及派生摘要差异同样需要明确记录。

bridge 受子预算、剩余总截止和输出上限约束；取消后向所属进程组发送 TERM，
最多 3 秒后 KILL，并等待 close，不再调度后续实验。JVM parser 自己负责独立进程组收束及临时目录
回收。公共 CLI 另提供声明截止加 10 秒的外层监督及最多 5 秒的强杀宽限。

## 判读与尚未满足的条件

报告分别展示：

- `evidenceValid`：原始证据重算及跨实验身份/时间检查是否通过。
- 单实验 `state`：unknown、equivalent-within-declared-budgets、uncertain 或
  false-positive-meaningful-change，附逐项效应区间、issues 和 warnings。
- `cohort`：计划数、可分类数、unknown 数、family 误判数、条件区间及完整计划最坏计数范围。
- `qualificationAccepted=false`、`performanceAccepted=false` 及其他校准未满足项。

单元格必须有完整原始证据、足够样本和区组、足够 bootstrap 分辨率、有限有效区间，且无 degenerate
等 warnings 才可分类。整族任一合格指标的相对和绝对区间均超出预声明实用差异界，计一次 family
实用变化误判；改善/退化两方向均计。所有指标在至少一种预算内才称等价；其余合格结果 uncertain。
不把等价或 uncertain 计作误判，也不把 invalid/缺项当负例。

另外单列 `familyExcludesZero` 诊断；微小但区间排除 0 的变化可能仍在实用等价界内。这里的误判计数
针对“错误宣称有意义改善/退化”，不能声称检验了所有零效应显著性检验的 nominal alpha。

二项区间采用 Clopper–Pearson 尾概率反演，数值以 binary64 计算并保留反演外侧边界。
区间假设 experiment-family 决策独立同分布；工具不自动证明该假设。缺坏项时可分类 cohort 区间仅
描述已分类部分，同时给计划分母下将 unknown 全计正/负两种极端的包络；不满足完整 cohort 和预声明
minimumExperiments 时，rateAssessment 保持 insufficient-evidence。

后续真实干净窗口、driver 余量、已知退化检出、collector/JVM/validator 成本、warmup 稳态和正式参数
冻结各有独立验收。当前首项不会把这些条件隐藏为已通过，也不接受手工填 `qualification=true`。

## 顺序 cohort 编排

`http calibration prepare` 将冻结的所有 experiment 逐一交给现有 `http comparison prepare`，
生成子 comparison task，并冻结一个上层 task。真实派发通过同一 `task run` 完成：

```sh
hgbench http calibration prepare cohort-runtime.json --plan FROZEN_CALIBRATION --out NEW_COHORT
# 使用上一步返回的绝对 taskPlan 和固定 request-id：
hgbench --root /absolute/cohort-tasks task run --plan /absolute/NEW_COHORT/task.json --detach --request-id COHORT_KEY
hgbench http calibration status /absolute/NEW_COHORT
hgbench http calibration stop /absolute/NEW_COHORT
hgbench http calibration report /absolute/NEW_COHORT/execution-set.json --plan FROZEN_CALIBRATION --out NEW_REPORT
```

prepare 只做本地验证和新文件冻结。run 需要非 root Linux 用户及已锁定的 Docker/Java/Node 环境。
每个 child 的 request ID 由同一 cohort attempt ID 与预声明 experiment ID 确定，随后始终查询该 task。
前一实验完成、原 task 身份匹配且干净窗口复查通过后，才派发下一实验。提交响应丢失会记录 unknown、
查询原 task 并进入有界清理；剩余项保留 not-started。本版支持原任务 status/stop，故障恢复始终沿用
同一身份。一个 cohort 只允许一次 dispatch；再次运行会拒绝，已有结果和分母保持可审阅。

运行时输入严格为以下字段；`comparisonRuntime` 使用已有 comparison runtime 的完整对象。
所有预算由操作者显式提供，无阈值、负载或耗时默认值：

| 字段                              | 约束                                                               |
| --------------------------------- | ------------------------------------------------------------------ |
| `schemaVersion/kind`              | `1` / `http-calibration-runtime`                                   |
| `comparisonRuntime`               | 既有严格 `http-comparison-runtime`，所有子实验共用；无任意命令钩子 |
| `cleanWindow.systemctlExecutable` | systemctl 的绝对路径                                               |
| `cleanWindow.requireClean`        | 必须为 `true`                                                      |
| `cleanWindow.units`               | 1–32 个唯一、显式的 systemd 用户 `.service` 单位名                 |
| `budgets.prepareMilliseconds`     | 1000–604800000；本地冻结总截止                                     |
| `budgets.maxRunMilliseconds`      | 1000–604800000；顺序执行总截止                                     |
| `budgets.pollMilliseconds`        | 1000–60000；等待原 task 的最短查询间隔                             |
| `budgets.commandMilliseconds`     | 1000–300000；单次 CLI 截止                                         |
| `budgets.maxCommandBytes`         | 64 KiB–16 MiB；单命令 stdout+stderr 总上限                         |
| `budgets.maxWorkBytes`            | 1 MiB–1 TiB；cohort 树的采样逻辑字节上限                           |
| `budgets.minFreeBytes`            | 0–1 PiB；采样磁盘保留空间                                          |
| `budgets.cleanupMilliseconds`     | 5000–20000；所有 child 停止、取消、最终查询共享截止                |

冻结检查 `maxRunMilliseconds >= cleanupMilliseconds + 60000 + Σ(child.task.timeoutMs + 60000)`，
且 `maxRunMilliseconds + cleanupMilliseconds + 10000` 不超过通用 task 的七天上限。上层 task 的
TERM 宽限固定 30s，足以容纳声明的最多 20s 清理和子命令回收；超出时保留清理未证实项。
文件空间为采样保护，独立 target/driver watchdog 继续负责容器最大寿命边界。

每次实验派发前和完成后都会对预声明单位执行 `systemctl --user show`，记录原始输出、实际 argv、
单调时钟、退出码和字段。只有退出码为 0、七个字段完整且唯一、Id 精确匹配，并且每个单位都明确
`loaded/inactive/dead` 或 `not-found/inactive/dead`、MainPID=0、空 ControlGroup，才允许派发。
每项 `stateEvidence` 分别记录 `loaded-inactive`（已加载且停止）或 `unit-absent`（unit 已被回收或不存在）；
原始 `Result` 保留为读取事实，`unit-absent` 不证明旧任务成功或其成功终态。读取失败、缺字段、重复字段、
非零退出码、load=error/masked、active、有 PID 或控制组等均拒绝，并记录 `unproven`。用户 bus 路径依据实际 uid
构造。该机制只观察声明的单位，其他后台进程和整个测量窗口的竞争仍需后续环境校准。
`wholeWindowExclusivityProven`、`qualificationAccepted`、`performanceAccepted` 始终为 false。

当前专用测试机的 runtime 声明这三个已停止或已被 systemd 回收的重任务；通用工具没有硬编码这些名字，
也不会停止或重新创建它们。受保护的 `hgbench-sf100-docker-environment-v6.service` 继续运行并在环境证据披露，
不纳入要求停止的列表：

```json
{
    "systemctlExecutable": "/usr/bin/systemctl",
    "requireClean": true,
    "units": [
        "hgbench-sf100-convert-v1.service",
        "hgbench-sf100-reference-convert-v1.service",
        "hgbench-sf100-update-audit-v1.service"
    ]
}
```

证据包括冻结的 workflow/lock、固定 child task 清单、每次预/后置系统状态、提交意图与回执、终态、
所有计划项的 result、相对引用的 execution-set、采样资源审计和 summary 锁。summary 锁仅绑定 cohort
汇总文件；每个真实 execution 的完整证据必须由现有 calibration report 独立验证。
stop 写入绑定 attempt/plan 的持久标记，只向原 active comparison 发 stop、向原 task ID 发 cancel。
重复 stop 安全；状态读取和中断恢复不会再派发实验。`taskTerminalProven` 只证明 task 终态，
容器回收仍须查看原 comparison/target 的证据和 watchdog 状态。

子任务 ID 在 prepare 阶段按公共 CLI 的 `taskRequestId` 规则预注册：request-id 为 1–128 个可见
ASCII 字符，先按 `JSON.stringify` 编码字符串（包含外层引号和转义），再对 UTF-8 字节计算
SHA-256，取前 32 个十六进制字符并加 `task-` 前缀。提交回执、按 request-id 查询的状态和取消
始终绑定这个预注册 ID 及原 task plan digest；收到不同 ID 时保留失败事实并查询原身份，不接受任意回执 ID。

本地故障测试覆盖实际 controller、公共 CLI 接线和合成 child CLI 协议。它们不启动 Docker/Java，
不构成真实 A/A 测量或噪声校准。首组真实参数仍须在干净窗口冻结；固定低速 open-loop 若每轮都
完成相同请求数，会产生退化的 goodput 效应分布，需要保留 unknown，不能据此判定重复性通过。
