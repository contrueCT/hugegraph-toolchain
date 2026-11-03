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

# HTTP 版本化预设

2026-10-25：用户已确认后续采用[HTTP / 图查询两条评测线](benchmark-tracks-v2.md)，
各自提供 quick / standard，共用闭环吞吐及轻量校准机制。HTTP 保留原 11 题并做版本化补充；
[图查询指南](graph-query.md)属于独立图查询线。
下面继续描述已冻结的 0.1.0 版本；新版使用独立的 [`http throughput` v0.2 入口](http-throughput.md)，已实现 quick / standard。旧 `http preset` 的 standard 仍按原设计状态解释，旧安装包和结果定义不变。

`http preset` 将一次保存的项目配置展开为现有的 HTTP、comparison 和 component-task 输入。
版本文件位于 `presets/http-v0.1.0/`，独立于已锁定的 HTTP 十一题规范包。
quick 的公开规划、真实 Docker 执行和报告重建已验证。首次真实 A/A 用时544.146秒，
1800个测量请求全部正确，完整证据回传与独立复核通过，见实际交付（本机记录未随包分发）。
这次三题筛查不构成正式校准或吞吐极限验证；版本文件中的验证状态是发布时快照，原字节保持。

此前已确认 HTTP 吞吐覆盖全11题逐题曲线及独立复测的最高观察正确吞吐，业务SLO可选。
下面0.1.0表格保留已冻结的档位定义；其中standard/full的原时间目标和指标草案不代表新容量规格。
原开放到达率容量规格、工程预注册参数与实现范围见[当时的吞吐方案](http-capacity-proposal.md)；
新版闭环默认方法以页首两轨决策为准。没有已验证的统一运行时长承诺。

| 0.1.0 档位 | 覆盖与负载                                                    | 配对与观测                                                                   | 范围与时间                                                | 当前状态                                                          |
| ---------- | ------------------------------------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------- |
| quick      | scalar-add、rows-4096、string-1024k；open 10 rps，最多4个在途 | 2个平衡配对、4个独立JVM；预热3s、测量15s；P50/goodput预声明至少100个正确观测 | screener/descriptive；P95仅描述；5–10分钟是未验证工程目标 | 公开plan/verify/report及同574真实正常/截止/取消已验证；无性能资格 |
| standard   | 十一题；待驱动余量校准后固定小型负载阶梯                      | 至少4个平衡配对；P99需每题/负载/物理臂至少1000个正确观测                     | 15–30分钟为设计范围                                       | design-only；plan明确拒绝，无fallback                             |
| full       | 十一题、identity/gzip、开放及闭环预声明分层                   | 独立实验、全族政策、足够P99样本及校准复用                                    | 小时级为设计范围，上限未定                                | design-only；plan明确拒绝，无fallback                             |

quick 的100个观测门专用于这个新版本的P50/goodput筛查。原有缺省P99政策和1000观测要求保持不变。
两配对的bootstrap结果不能证明稳定性、可信显著性、细微胜负或性能资格；本入口不产生p值。
`statisticsAccepted` 和 `performanceAccepted` 始终为false。原报告中的问题、warning、低样本、drop和失败臂继续保留。

## 保存一次项目配置

从 `examples/http-project.json` 复制项目JSON，设置四份已有配置的路径；相对路径基于项目JSON自身。

```json
{
    "schemaVersion": 1,
    "kind": "http-benchmark-project",
    "id": "my-hugegraph-http",
    "label": "My HugeGraph HTTP project",
    "description": "Actual candidate and dedicated test environment",
    "comparison": "forward-embedded",
    "aaMode": "forward",
    "sources": {
        "build": "./project/build.json",
        "settings": "./project/environment-settings.json",
        "environment": "./project/host-environment.json",
        "runtime": "./project/comparison-runtime.json"
    }
}
```

`build` 是既有构建及文件清单，`environment` 是实际主机/镜像/Node证据，
`settings` 保存目标CPU、内存、JVM、服务线程和磁盘预算，`runtime` 保存driver、collector和supervisor资源。
这些文件的完整原字节及SHA进入新计划，无需每次手写profile和comparison请求。
仅将settings中的初始mode交给每臂计划选择，并把purpose显式设为functional；其他资源设置保留。
使用 `comparison=aa` 时两臂采用 `aaMode`，仍是quick筛查，不能替代正式A/A校准。

runtime 的 `sourceRoot` 必须与执行 `comparison prepare` 的工具源码一致，Node/JDK/镜像也须经原prepare核验。
旧v6冻结runtime不会自动改写成新工具来源；新P50计划配旧工具会被既有prepare明确拒绝。
预览列出sourceRoot是否匹配当前工具，但 `executionReady=false` 始终保留到原prepare及真实执行检查。
规划不会启动容器、检测当前端口、部署数据库或代填实际环境证据。

## 预览、冻结和执行

```sh
node dist/cli.js http preset list
node dist/cli.js http preset show quick
node dist/cli.js schema http-project
node dist/cli.js http preset plan --project /absolute/project.json \
  --out /absolute/quick-001 --dry-run
node dist/cli.js http preset plan --project /absolute/project.json \
  --out /absolute/quick-001
node dist/cli.js http preset verify /absolute/quick-001
```

省略 `--preset` 使用quick。dry-run经现有规划器在有界临时目录展开、验证并清理临时产物，
不会创建 `--out` 或发送HTTP/Docker请求。它的 `planDigest=null`，不能当作已冻结计划。
实际plan必须使用新目录；异常保留partial和failure.json，禁止覆盖原目录重试。

```text
project JSON + versioned quick
             |
generateHttpProfile -> planHttp -> comparison plan
             |
 frozen profile + preset evidence + comparison + runtime snapshot
             |
 existing comparison prepare -> component task run/status/wait/cancel
             |
 original sealed execution -> existing raw report -> thin preset HTML/JSON
```

计划返回可复制的原有prepare nextAction。后续命令沿同一个生命周期：

```sh
node dist/cli.js http comparison prepare /absolute/quick-001/project/runtime.json \
  --plan /absolute/quick-001/comparison --out /absolute/quick-001-attempt
node dist/cli.js --root /absolute/tasks task run \
  --plan /absolute/quick-001-attempt/task.json --detach --request-id quick-001
node dist/cli.js --root /absolute/tasks task status --request-id quick-001
node dist/cli.js http preset report /absolute/quick-001-attempt/execution \
  --plan /absolute/quick-001 --out /absolute/quick-001-report
```

真实执行要求原有Linux/Docker受控环境。断线后只查原request-id/attempt，不重复派发。
没有新增执行器、队列、自动重试或第二套run/status语义。

## 时间、样本和预算

固定预热与测量基线是 `4臂 × 3题 × (3+15)s = 216s`，其中预热36s、测量180s。
settle固定等候另外列出；drain仅列安全余量。前后探针、JVM启动、prepare、分析、停止等开销独立列出。
`normalEtaMilliseconds=null`、confidence=low；未知开销不填0，也不由历史功能记录假定吞吐或启动速度。

quick每题/臂计划150次measurement到达，总measurement计划1800次。它是正确观测的上界，
实际送达、成功、正确、窗内完成和驱动余量均未预测。采样不足和quality失败保持原unknown/invalid。
1MiB题完整校验和trace保持开启。P95为原HTTP报告描述值；150次上界不支持1000观测的P99门。

当前新计划默认 `--budget-mode total`，允许显式选择 `stage-sum`。原已冻结计划沿自己的预算规则读取。
`total` 中的 `--budget-ms` 是整次运行共享截止，包含启动、预热、测量、清理与封存；
规划、事前 prepare 和事后离线报告分别使用自己的预算。每个阶段仍有独立上限，
可用时间取阶段上限与整任务剩余时间的较小值，并保留收尾时间。预算耗尽保留partial和完整四臂，
不会为赶上截止删题、缩窗口或减少配对。

`total` 的最低预算按固定工作、停止与必要收尾保留推导；`stage-sum` 另外要求覆盖各阶段最坏余量之和。
旧版约4156000ms是阶段最坏上界参考，不能当成当前total的最低预算或预计耗时。
省略 `--budget-ms` 时仍继承0.1.0预设的4200000ms；其解释由选定模式决定。
建议显式配置实际可接受上限，例如 `--budget-mode total --budget-ms 1200000`。
预算不足返回 `HTTP_PRESET_BUDGET_INSUFFICIENT` 和按当前模式推导的 `requiredBudgetMs`。
预算允许规划不保证目标环境会在截止前完成。

同574版本已通过真实正常quick、total截止和测量中主动取消；执行记录保留在本地工作区。
这些证明生命周期与保留部分结果的行为，不能代替性能校准。
standard/full未实现时返回 `HTTP_PRESET_NOT_IMPLEMENTED`，不会偷偷转成quick。
其他稳定错误包括 `HTTP_PRESET_UNKNOWN`、`HTTP_PRESET_PROJECT_FILE`、`HTTP_PRESET_OPTIONS_INVALID`、
`HTTP_PRESET_OUTPUT_EXISTS`、`HTTP_PRESET_PLAN_CHANGED` 和 `HTTP_PRESET_BINDING_INVALID`。

## 绑定与报告边界

`preset-binding.json` 记录版本、定义SHA、实际三题子集、参数、项目来源及限制，作为现有target evidence附件
进入comparison内容锁和条件摘要。`preset.lock.json`再锁定完整公开计划；verify复核原comparison及预设参数匹配。
报告入口先校验该绑定，再调用原raw verifier/analyzer，生成带明确范围的导航HTML和preset-report.json。
原配对HTML/JSON位于comparison子目录，全部原门和全四臂分母保持。

资源、细时钟归因、预热稳态、观测成本、driver余量和正式性能验收各有独立证据要求。
quick可用于发现明显异常并检查工具链，不能以命令成功或两配对完成代替这些验收。
