# 三场景驱动余量与预热过渡

`hgbench http calibration driver-headroom` 独立读取headroom002原始证据，按执行前固定的 `headroom-engineering-3case-v1` 给出限定范围的工程判定。它覆盖scalar-add、rows-4096、string-1024k，open10/20/40，驱动CPU配额1/2核和两个反序区组，共12个JVM臂、36个测量单元。

```sh
hgbench schema http-driver-headroom-evidence-input
hgbench http calibration driver-headroom --help
hgbench http calibration driver-headroom input.json \
  --out new-report --timeout-ms 600000 --max-bytes 33554432
```

输入必须绑定原plan/spec/source、工程判据、实际assessment binding、原派发前launch记录及原controller启动证据。路径相对于输入JSON。判据只接受固定内容摘要；launch引用binding摘要，输入再引用launch摘要，避免循环哈希。实际计划冻结后、任何负载产生前，必须完成这条绑定。启动后立即保存原systemd的启动单调时刻/PID和同PID的proc stat；核对PID、startTicks和boot身份，并验证launch早于该controller启动。它们仍属于producer原始记录。原headroom002输入、执行器和结果保持原义；缺少事前绑定及启动证据不能取得工程通过。

| 检查           | 通过条件与解释                                                                                                                            |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 身份与原始质量 | 原始HTTP、目标环境、任务、容器、collector和JVM证据核验；完整36单元，预热及测量零错误、丢弃和迟到，每个测量单元至少1000正确样本            |
| 驱动压力       | 每个完整phase的平均进程CPU不超过0.8核、ELU不超过0.8；CPU由user/system时间和elapsed重算，ELU为来源绑定记录值，原件没有active/idle计数      |
| CPU敏感性      | 同case/rate/block的CPU1与CPU2整段端到端P99，差值不超过max(1ms,10%×较小值)，共18项                                                         |
| 重复观察       | 同case/rate/CPU跨两个block的P99使用相同容差，共18项；该容差是前瞻工程规则，未给统计等价保证                                               |
| 预热过渡       | 最后两个预热10秒窗和最初两个测量10秒窗，分别比较response P50/P95的窗分位数中位数；每窗至少50样本，差值不超过max(1ms,10%×预热参考)，共72项 |
| 完成窗口       | 披露正确完成吞吐、backlog和原计数；没有额外95%完成阈值，也不据此判定完整测量段稳态                                                        |

所有计划单元均保留。未执行、无效、缺项或样本不足会保留unknown；已验证的阈值超限同时保留failed及原值。完整且满足工程判据的证据可给 `limitedScopeEngineeringAccepted=true`，适用范围严格限定在上述三题、速率、资源和观测栈。

全11题覆盖、80rps或更大速率、120秒完整稳态、目标服务容量和真实误报率认证仍未取得资格。`inferenceAccepted` 与 `performanceAccepted` 保持false；窗口P99样本不足时保留unknown。ELU和Node phase CPU含drain，与整个驱动容器的CPU端点计数分别报告。

该入口只读本地证据、运行有界分析并写新报告，不发送HTTP或执行保存的Docker命令。`--max-bytes` 是输出上限，完整输入清单另有固定16GiB上限。组件的总截止包括工作进程清理和收尾；CLI另有最多5秒监督和5秒强制清理边界。退出0只表示报告生成，仍需检查状态、完整分母、适用范围和报告锁。

真实headroom002已完成执行、完整回传及独立判定（本机记录未随包分发）。
新629消费者核验通过10个已执行臂，原12臂/36单元完整保留，两个未执行臂的六单元仍unknown；
工程判定failed，`limitedScopeEngineeringAccepted=false`。原611的Docker挂载顺序误拒报告保留，
当前结论以629 reader-v2为准。新631工程汇总（本机记录未随包分发）
另已验证44条匹配失败、缺项及逐题范围的传递，未授予完整资格。协议夹具与真实实验分别留证。

以下是原执行已完成的派发绑定流程，供解释证据来源；原任务不重派：

1. Linux prepare/freeze完成后，创建独立assessment binding，固定criteria SHA `71fe2c244e0308d451e2759c06a457d576cf9afb73c526b00dfb978d4b524a0a` 和实际plan/spec/source摘要及原目录。binding只引用launch记录路径，避免相互摘要循环。
2. 启动前写create-only launch JSON，字段为 `schemaVersion:1`、`kind:http-headroom-engineering-launch`、`operationVersion:http-driver-headroom-002`、`bindingSha256`、`planLockSha256`、`executionSpecSha256`、`sourceSelectionSha256`、`hostBootId`、`startedMonotonicNs`、`finishedMonotonicNs`、原executor的 `argv`。
3. 单次启动后立即采样原unit，保存create-only controller-start JSON：`schemaVersion:1`、`kind:http-headroom-controller-start-evidence`、`hostBootId`、`observedMonotonicNs`、`systemd:{argv,stdout,exitCode}`、`procStatRaw`。原systemd命令只读 `hgbench-http-driver-headroom-002.service` 的 `MainPID`、`ExecMainPID`、`ExecMainStartTimestampMonotonic`、`ControlGroup`、`ActiveState`；proc stat来自这个MainPID。
4. 原dispatch的PID/startTicks/boot、systemd原属性和proc stat必须吻合，launch结束早于systemd记录的controller启动。原002的dispatch没有单调开始时刻，不得用第一条工作命令冒充该字段。读报告的input固定launch和controller-start文件的SHA；缺少实际采样不能补写为已验证。

这些记录须与原执行一起回传。若派发响应丢失，查询原unit/claim，不重新启动或覆盖记录。
