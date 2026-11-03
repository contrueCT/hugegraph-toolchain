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

# HTTP 与图查询两条评测线

2026-10-25，用户已确认本次范围：HTTP 性能与图查询性能分别提供 quick / standard，
共用闭环吞吐方法、轻量校准、执行器和报告组件。本文记录新版设计决策；HTTP v0.2 的当前实现及验证见[使用说明](http-throughput.md)，
图查询接入与完整测量状态见[阶段一验收边界](phase1-acceptance.md)。已有版本、原始证据和安装包保持原定义。

```text
                        HG-Bench / TS CLI
                               |
              +----------------+----------------+
              |                                 |
        HTTP 服务性能                       图查询性能
     原 11 题 + 小幅补充                 真实大图 + 5 类查询
       quick / standard                  quick / standard
              |                                 |
              +----------------+----------------+
                               |
           Go 执行器 / 并发阶梯 / 校准复用 / 逐点检查
                               |
             原始证据 / 分轨对比报告 / 单图 SVG 导出
```

## 范围与档位

| 评测线        | 测量对象                                                           | quick                                                | standard                                               |
| ------------- | ------------------------------------------------------------------ | ---------------------------------------------------- | ------------------------------------------------------ |
| HTTP 服务性能 | 请求处理、简单脚本执行、结果构造、序列化、压缩与传输组成的服务路径 | 小标量、多行、较大响应的代表子集；少量并发点、短测量 | 覆盖原 11 题及版本化补充；更细并发阶梯、持续测量和复测 |
| 图查询性能    | 指定数据、查询语义及参数分布下的端到端查询性能                     | 同一大图上的五类查询；减少并发点、时间和轮数         | 同五类查询，更完整曲线和复测；混合负载可选             |

HTTP 的 11 题继续作为独立基准题集，适合比较服务入口、HTTP 容器、编码和响应规模。
它们不读取图数据，不用于推断图遍历或存储性能；结果包含简单脚本和结果构造的开销。
图查询若通过 HTTP 调用，其结果同样包含接口与传输开销，报告标明调用入口和后端。
两条线可结合解释瓶颈，各自保留成绩，不合成跨轨总分。

HTTP 题集的小幅调整包括：

- 保留原 11 题的身份与语义，quick 从中选择代表子集，standard 保持完整覆盖。
- 补充确定性的较低可压缩响应，避免只有重复字符的大响应代表全部传输场景。
  原重复字符题继续用于观察高压缩比场景；新增内容使用新 case ID 和题集版本。
- 显式记录 identity / gzip、解压后大小及线上传输字节；不同编码分别展示。
  精确补充题目、默认编码和 HTTP 档位参数在实现规格中冻结，不能套用图查询耗时估算。

图查询采用[图查询指南](graph-query.md)中的五类：按 ID 点查、
一跳出邻居、共同出邻居、三跳内最短路径、Gremlin 一跳邻居。原生图 REST 指直接调用
HugeGraph 的顶点或 traverser API；Gremlin 则通过脚本入口执行。二者的语义分别固定。
SF100 规模要求保持；quick 与 standard 使用相同数据快照、参数池和参考答案。
Twitter-2010 参考场景映射到 SF100 时需明确边标签、方向和分层参数，不能直接复制其成绩。

图查询建议初始配置为 quick 三档并发与候选点持续测量，standard 六档并发与候选/邻近点
各三轮复测，具体参数可配置。单构建、单编码的固定窗口分别约 10 分 25 秒、52 分 30 秒；
这来自配置计算，尚非实测耗时。导入、参考准备、首次校准和生命周期成本另列。

## 共用方法与轻量校准

两条线的新版主路径均为固定并发闭环：每个客户端完成一个请求后发出下一个请求。
逐级增加并发，观察正确吞吐、延迟与错误，选择候选平台后在新时间窗持续测量或复测。
最高档仍在增长就报告“平台未找到”；驱动、错误或预算受限时标记相应限制。
固定到达率保留为延迟 / SLO 的补充方式。quick 与 standard 的差别主要是覆盖和测量深度。

轻量校准默认使用 1–2 类代表检查：短响应高请求率，以及实际负载中较重的结构/响应。
初始建议每类 10 秒预热 + 30 秒测量。复用机制共享，具体校准证据按适用范围复用：

- 缓存绑定执行器版本、主机资源、网络/连接、编码、参数选择、答案校验和观测方式。
- 校准记录包含已覆盖的请求率、响应结构和大小；HTTP 的固定响应校验通过，不自动覆盖
  图查询的参数池选择和语义校验开销。越界或配置变化时补相应检查。
- 每个真实测试点继续检查答案、错误、实际并发、驱动资源与写入压力，以及目标 GC 和宿主 swap。
  正式测试关闭 HG 内存监控触发的手动 GC，或记录获准的高阈值，并验证 swap 已关闭。
- A/A、已知退化检测和统计认证保留为发布、异常诊断或专项验证能力；默认每次运行不安排
  逐题长校准。代表检查通过的结论限定在覆盖范围内。

保留原始数据、失败和未完成项。主指标为测量窗口内正确完成的请求数/秒，
同时报告延迟、样本数、错误、窗后完成及逐轮离散程度。完整答案校验保持启用。
整任务预算仍为默认范围，可配置；dry-run 展示固定工作量、准备成本和硬截止。

## 实现与交付边界

TS CLI 负责组织入口与调度，Go 负责请求执行，分析和离线 SVG 报告复用现有组件。
不新增常驻平台、前端框架或另一套任务系统。后续实现分为：

1. 共用并发阶梯、候选点复测和轻量校准缓存；在现有 HTTP 请求上接通新版 quick / standard。
2. 图查询的 GET / POST 请求模板、分层参数池、独立参考与语义校验，接入同一执行路径。
3. 两轨报告与配置入口联验；使用真实大图验证图查询两档，保留数据准备的独立成本。

报告身份至少包含评测线、题集/语义版本、数据与参数摘要、档位/执行计划、负载模型、编码、
执行器和环境。多组报告按兼容条件分组；跨轨或语义不一致的数据可以并列浏览，
不计算统一排名或误导性的提升比例。已有逐图 SVG 导出继续共用。

旧 `http-v0.1.0` 的 quick、standard/full 草案与旧校准结果按原版本解释。
本决策不使旧 standard 入口自动可用，也不改变旧 `performanceAccepted` 状态。
新版仍需实现、功能验收与适用范围内的真实验证。完整阶段一未完成项继续保留，
本次设计确认没有启动远端任务。
