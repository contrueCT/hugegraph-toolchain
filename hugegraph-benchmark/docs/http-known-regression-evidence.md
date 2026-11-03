# 已知退化的离线证据判定

`hgbench http calibration known-regression` 从 known-delay002 的原始证据重建一个限定范围的检出结论。它固定使用 `1 + 1` 与 `Thread.sleep(10); 1 + 1`、16个独立JVM臂、8个平衡配对、端到端P99和5%/5ms阈值，参数来自执行前冻结的计划。

```text
原计划 + 执行规格 + 原始HTTP/目标环境/命令记录
                        |
                 独立读取与核对
                        |
          16臂完整分母 → 8配对 → P99效应区间
                        |
             passed / failed / unknown
```

公开入口：

```sh
hgbench schema http-known-regression-evidence-input
hgbench http calibration known-regression --help
hgbench http calibration known-regression input.json \
  --out new-report --timeout-ms 300000 --max-bytes 16777216
```

输入包含 `planDirectory`、`executionDirectory` 和执行前固定的 `planLockSha256`、`executionSpecSha256`、`sourceSelectionSha256`。相对目录相对于输入JSON解析。`operationVersion` 固定为 `aa005-async-end-to-end-p99-v1`，源码清单固定为A/A005的529个成员；接口拒绝自定义脚本、阈值或人工 `passed` 字段。原始目录可以搬移，原件中的绝对路径保持原字节并按身份核对。

读取过程重算HTTP原始记录的正确性、计数和P99，检查固定profile、实际Node与测量源码摘要、目标模式/构建/资源、原任务身份、容器归属和正常结束证据。保存的分析报告或执行器的 `verified` 布尔值不会替代这些核对。原始清单在读取前后必须一致；不会执行证据中保存的命令。

完整且合格的8配对中，效应区间同时超过5%和5ms阈值才给该检出门 `passed`；合格区间明确未检出则 `failed`，缺失、无效、样本不足或区间不确定均为 `unknown`。未执行draft保留16臂/8配对分母，状态为 `unknown`。命令退出0表示成功生成报告，需检查报告中的状态。

输出为 `input.json`、`report.json`、`report.lock.json`。失败或取消尽量保留 `partial.json`，并保留完整计划分母；已有输出目录不会覆盖。`--max-bytes` 控制新输出大小，原计划和执行目录的合计输入清单另有固定8GiB上限。`--timeout-ms` 覆盖组件读取、计算、工作进程清理及收尾；CLI外层另有最多5秒监督和5秒强制清理边界。硬截止失败不能算有效测量。

结论只适用于当前forward算术控制、open20/IF4、JVM观测与持续collector关闭时的端到端P99敏感性。它不提供其他11题、全观测栈、1ms检出、吞吐容量或真实误报率认证；输出的 `inferenceAccepted` 与 `performanceAccepted` 保持false。529成员源码清单和实际测量组件摘要已绑定，完整物理源码树与依赖未在此入口重新验证；原命令记录也不构成外部执行认证。当前容器是否仍存在需另作实时归属检查。此入口核对pre/run/post命令次序及目标身份；raw测量时钟到宿主命令时钟的精细映射尚未独立建立。

本地协议测试包含合成生命周期、配对计算与缺项反例。known002实际执行与回传已完成（本机记录未随包分发）：16臂、19,200测量及3,200预热全部正确，配对P99增量10.305ms，独立公共reader给出限定范围的passed；该结论来自真实证据。独立报告不会改写A/A005的原报告或工程总验收状态。
