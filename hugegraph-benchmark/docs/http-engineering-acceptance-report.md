# 汇总工程验收证据

`hgbench http calibration acceptance` 在新目录生成七个工程门、完整11题覆盖表、离线HTML和校准引用索引。它把已验证的贡献与适用范围一起汇总，保留缺项、失败和不确定项；旧cohort报告不改写。

```sh
hgbench schema http-engineering-acceptance-input
hgbench http calibration acceptance --help
hgbench http calibration acceptance input.json \
  --out new-acceptance --timeout-ms 600000 --max-bytes 134217728
```

输入路径相对于输入JSON解析。每项尚无证据时显式写`null`；[空输入示例](../examples/http-engineering-acceptance-input.json)会生成完整的unknown报告。已有known/headroom/observer证据分别填写原reader输入JSON及其原字节SHA：

```json
{
    "inputFile": "known-input.json",
    "sha256": "原输入文件的64位小写SHA256"
}
```

三个reader会从原始记录重新核验。输入不能包含人工`passed`、阈值覆盖或脚本命令。进程退出0表示报告成功生成，判定需读取`engineering.status`、各门的scope和逐题覆盖。

A/A有两种来源，至多选择一种。`repeatabilityAa`接受当前版本的`planDirectory`、`planLockSha256`、`executionSetFile`与`executionSetSha256`，复用原cohort公共验证器独立重验raw。原计划的报告时间和字节预算必须能放入当前剩余额度；不足就明确拒绝，不缩减实验或改原阈值。

历史`aa005`入口只支持已封存的A/A005证据根目录：`{"evidenceRoot":"aa005-evidence"}`。组件核对固定身份的原始文件清单、529成员来源、两次独立实验及原独立重建证据链，再复算已有实验判断；它不会重新派发实验，也不将这一过程称为新一次raw独立重放。A/A005原uncertain继续保留。其他cohort不能靠提供同形JSON或`passed`替代此历史证据链。

```text
原A/A005证据与已完成的独立重建 ─┐
known / headroom / observer raw ─┼─ 七门 + 11题覆盖 + 明确限制
可选正式comparison计划 ─────────┘               |
                                 离线JSON/HTML + 校准引用索引
```

时间预算由全部reader和收尾共享；字节预算覆盖新输出及临时文件。各reader原有输入读取上限保留，A/A005另有固定2GiB输入读取上限。取消或超时保留部分结果与完整缺项，不用已完成子项填满其余分母。CLI另有最多5秒监督余量和5秒强制清理边界。

三题余量或观测成本结论不会变成另外八题通过，高速率和embedded模式也需各自适用证据。已知退化沿原固定算术sleep10正控制；观测成本按有效测量并披露解释，非零成本本身不增加新的拒绝门。预热过渡检查与完整测量段稳态分别说明。可选误报率认证与工程验收保持独立，性能和qualification资格仍为false。

部分执行也保留可核实的失败。headroom 的原计划、完整分母、已执行臂身份及原始记录核验通过后，报告逐项保存已证实的质量、CPU/事件循环压力、预热过渡和P99一致性失败；未执行臂与其余题目继续unknown。`observedEvidence`说明计划/已核验/缺失数量，`observedFailures`绑定原臂、题目、原始清单摘要和具体数值。这条路径只传播匹配的失败，`rawVerified`仍为false，不生成任何passed门或完整测量资格。

校准索引不反向引用正式comparison计划，便于先将索引放入现有`profile.target.evidenceFiles`再冻结比较。可选comparison输入指定`planDirectory`、`planLockSha256`、`calibrationIndexFile`与`calibrationIndexSha256`；报告核验复制锁定及参数覆盖。构建、题目、模式、负载、资源或观测栈不匹配时逐项列出，缺少可核字段保持unknown。参数锁定与测量资格分别报告。

observer003的条件包含锁定的JVM观测开、关两套`settingsVariants`及16个运行臂的对应关系，通用`settings`为null。比较同时核对实际配置、collector/JVM开关组合与资源，未测量的“双关闭”组合保持不匹配。其他实验仍使用单一`settings`；缺少或损坏的变体、运行臂映射不会被替代配置掩盖。

已用独立安装包`engineering-consumer-20261024.1`完成一次真实证据汇总（本机记录未随包分发）：原A/A005证据链保持unknown，known002原始记录重新核验后限定正控制passed，缺少的余量、观测成本、稳态和正式冻结保持unknown。七门、11题完整保留，总工程/性能/qualification均未接受。当前cohort适配器另有公共验证器协议与取消检查，尚未用新cohort真实测量数据完成此新汇总入口的正向验收。

随后补正正式比较的条件匹配：known固定算术正控制与正式题集的题目、负载及观测配置差异进入非阻塞披露，共同构建、主机及测量源码仍核对；其方法门不额外要求全11题正控制。环境组件锚也更正为实际的`hugegraph-environment.py`。该增量已进入独立包`engineering-consumer-20261024.2`，并完成7步真实索引接线（本机记录未随包分发）和独立复核；旧626索引、比较计划内副本及新汇总索引字节相同。此次离线锁门通过，A/A对embedded的失配与7项未知保留，整体工程/性能资格仍为false。旧`.1`包和报告保持原件。

源码中的HTML展示另已补齐比较失配、未知条件及方法控制披露，11题可展开查看原七门覆盖和限制。4项纯渲染测试通过；渲染预览（本机记录未随包分发）仅从已验证的小报告生成，独立预览锁封其新HTML，原来源锁另存。此展示增量尚未进入上述`.2`安装包，没有再次读取raw，也未做图形浏览器布局验收。

`calibration-consumer-20261024.3`包含上述HTML展示和部分失败保留修复。真实headroom汇总（本机记录未随包分发）一次公开调用耗时8.496秒，完整12臂/36单元中10臂/30单元核验有效，两臂/六单元缺失；44条匹配失败进入JSON、索引和HTML。三个代表题的驱动门failed，scalar/rows的预热门failed，string的预热门unknown，其余八题unknown。此报告只接入原headroom证据，其他校准输入显式为空。

observer003独立交付（本机记录未随包分发）已完成：16臂/48题次全部有效，三题条件成本完成测量，其余八题未知。首次四类证据汇总接线（本机记录未随包分发）因未识别两套settings而partial，原报告保留；上述变体修复的9项针对性检查及独立源码审查通过；新634安装包已一次20.485秒生成四类证据完整汇总（本机记录未随包分发），partial=false、issues为空，原失败与未知范围完整保留。接线完成后总体工程仍failed，性能与qualification接受为false。
