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

# 阶段一验收对照

2026-11-03，依据[阶段定义](stages.md)。当前结论：**阶段一未完成**。下表保留完整范围；五类图查询、HTTP、官方完整 LDBC track 的证据分别解释。五题图轨支持用户选择导入完整度，当前SF100执行门槛为95%；[数据快照规则](graph-query.md#数据完整度)记录实际范围及证明依据。

| 完成条件                    | 现在能够证明的范围                                                                                            | 未完成部分 / 应取得的证据                                                                                                                     |
| --------------------------- | ------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. 独立、版本化的规范与题集 | 独立 `0.1.0-draft.3` 规范校验124文件/29类操作通过，归档锁与来源固定                                           | 仍为候选，`releaseReady=false`；需要完成发布门与仓库审阅                                                                                      |
| 2. 固定参数、参考与拒错规则 | 29类参数/结果契约和严格有序验证组件已实现；五类图查询每题128条独立参数/可接受答案已冻结                       | 五类答案仅覆盖Person/knows投影；目标全图正确性、完整29类SF100有序参考和更新后状态仍需实际验收                                                 |
| 3. 固定正式测量协议         | HTTP/图轨分别有quick/standard、负载/重复/计时/答案/证据边界；当前HStore构建与资源配置冻结；图轨支持可配置导入门槛及实际快照 | 五题轨等待所选完整度及查询依赖关系闭合；真实图两档、适用范围的驱动与环境校准和参数冻结待完成；完整LDBC track的全量符合性证据仍缺失 |
| 4. 成绩卡、有效性与比较规则 | 独立scorecard/流式答案校验器及分轨报告已实现；错误/缺项保留，图五题不合成总分                                 | 没有SF100有效成绩；固定规范的调度/有效性规则须由完整真实证据证明，不能从功能测试推断                                                          |
| 5. 真实适配、校准与独立复现 | 完整29操作官方driver及严格验证入口通过本机/真实Linux JDK17协议检查；HG memory语义组件及HStore部分导入核验通过 | 五题轨：所选快照准入→quick/standard；完整LDBC track：全量符合性及真实有序验证。相应A/A/已知退化/驱动余量及另一环境或使用者复现仍须真实证据 |
| 6. 可审阅版本化候选与证据   | 有独立规范候选与可安装工具快照，原始失败和分片证明保留                                                        | 全部发布门的实际证据、复现命令与仓库审阅/公开发布待完成；工具封包不等于规范正式发布                                                           |

## 证据与复现边界

本页随工具分发，记录验收要求和仍待完成的范围。本机运行日志、环境交接、原始证据与旧安装快照保留在执行工作区，按当前分发规则从新源码快照和npm包中排除；旧快照保持原件。使用者应由自己的实际执行生成并保留这些证据；不能将本页状态、组件测试或协议夹具作为完整SF100验收结果。

- 输入清单、转换摘要和Loader提交回执证明各自环节。五题图轨分别披露已确认记录比例、内容核验覆盖和查询答案正确性；采用回执路径时不宣称全图内容通过。完整LDBC track的全量符合性要求保持。
- 五题图查询的参考仅覆盖Person/knows投影；完整29类LDBC有序验证与更新后状态单独验收。
- Linux组件接线、官方driver的协议检查和本地安装检查证明工具可运行；真实性能还须满足正式环境、正确性、驱动余量和适用范围的工程校准。
- 独立复现需要另一环境或使用者实际完成规定链路，并提供版本、配置、命令及原始结果。

## 五题图轨的数据与测量门

```text
固定完整SF100源输入 + 五题独立参考
                 |
      HStore及全部分组就绪 → 数据/epoch/GC检查
                 |
       全部顶点写入确认 → 默认全顶点内容门
                 |
       边记录比例达到所选门槛（当前95%，可选100%）
       + person_knows_person完整闭合
                 |
       固定实际导入快照、各类型缺失数及核验依据
                 |
       停写 → 恢复正式超时/新Server JVM → GC/swap检查
                 |
          图查询 quick → standard → 报告
```

当前轻量路径复用已完成的全顶点内容证明，以成功Loader回执计算已确认边记录比例，不重复全顶点扫描或最终全边内容扫描。未知写入结果不计入比例；查询答案仍逐条校验，95%的导入门槛不放宽答案规则。部分导入快照按实际范围命名和比较，不能作为完整SF100或完整29操作轨的符合性证明。

选择`full-content`时保留完整顶点/边内容、schema、索引和数量核验流程。五题图轨完成后仍须核对真实结果与工程校准范围，完整29操作轨和整体阶段一发布门分别取得实际证据。缺失条件保持未完成，不从功能检查或一次成功执行推断验收通过。
