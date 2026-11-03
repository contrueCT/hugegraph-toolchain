# 三场景观测成本的独立读取

`hgbench http calibration observer-cost` 从observer003原件独立重建JVM观测和资源collector的条件成本。范围已获用户确认：scalar-add、rows-4096、string-1024k；其余8题的观测成本明确unknown，正式吞吐及forward/embedded仍保留11题。

```sh
hgbench schema http-observer-cost-evidence-input
hgbench http calibration observer-cost --help
hgbench http calibration observer-cost input.json \
  --out new-report --timeout-ms 600000 --max-bytes 33554432
```

输入固定 `operationVersion=http-observer-cost-003`，包含原计划/执行目录，以及plan锁、execution规格、529成员来源清单的SHA。相对路径相对于输入JSON。只接受本次事前冻结的scope、顺序、策略和原始任务身份，不接受手写通过标志。

```text
J：collector保持开启，比较JVM观测关闭/开启
C：JVM观测保持开启，比较collector关闭/开启
                  |
      各4个新配对，共16个独立JVM臂
                  |
      3题 × E2E P99 / response P50
                  |
   每组6指标的效应区间、质量与完整分母
```

答案校验和异步trace始终开启。这两组条件效应不能相加，不能推导同时关闭所有观测的总效应；validator和trace的反事实总成本仍未知，已有自耗诊断单独披露。

原始HTTP记录、目标模式与资源、原任务/命令、确切容器归属、collector及JVM原件分别核验，再重算原配对统计。启用/关闭分支必须由实际配置和原执行记录支持，`off.json` 或私有verified标志本身不足以证明。保存的命令及内嵌脚本只作核对，不会被执行。

`scopedCostMeasured=true` 表示该范围内的成本已有效测量并披露。原质量及推断条件满足时，观察到成本或区间跨过预算也会如实展示；没有新增“成本必须低于阈值才能运行工具”的要求。无有效区间、样本不足、缺臂或原始证据无效时保留unknown，完整16臂分母不缩减。判定与每个指标的成本关系分开显示，不能把工程通过解读为零成本。

full11覆盖、容量、精细raw到host时钟关联和统计误报率认证未取得资格。报告的 `inferenceAccepted` 与 `performanceAccepted` 保持false；实际当前资源清理还需独立实时归属检查。

`--max-bytes` 是新输出上限，输入清单另有固定32GiB上限；总组件截止包括读取、工作进程回收和收尾，CLI额外留最多5秒监督及5秒强制清理边界。只写新目录，失败尽量保留partial；退出0表示报告生成，需检查状态、报告锁、每组区间及限制。

observer003已完成原单次执行、归属清理、完整回传和独立测量，见完整交付（本机记录未随包分发）。16臂/48题次全部有效，57,600次测量及14,400次预热全部正确，无失败、丢弃或迟到；48个trace writer完整。32个原容器及72个捕获host PID均有不存在证据，其中64条带start ticks、8条仅原Docker PID，不虚构容器内到host的身份映射。

首次固定611读取因Docker `stop --time`在stdout输出弃用提示而拒绝CID整串比较，原unknown报告保留。窄修只允许已锁定stop命令的精确提示加同完整CID，其他命令/未知内容保持严格校验；3项定向检查和独立审阅通过。新632独立读取全部16臂有效，J/C每组6指标均有4配对；每组3个P99及string P50跨数值预算，其余两个P50为within-at-least-one-budget。`scopedCostMeasured=true`只授予该范围的有效成本测量与披露，统计等价、零成本和性能资格均未授予。

原计划固定预热与测量2小时，实际原执行8,828.620秒（约2小时27分，含启停和分析收尾）。active上限30000秒、异常共享清理最多另90秒、parent上限30100秒和停止宽限100秒均保持。该私有实验预算沿用原执行器语义，与新容量CLI默认`total`模式分别记录。后续新运行仍须重新绑定事前计划与实际环境；不能重派这个原实验填补其他配置的证据。
