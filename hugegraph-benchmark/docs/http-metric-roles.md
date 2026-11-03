# HTTP comparison v2 的指标角色

固定低负载可以比较延迟。在这种实验中，正确吞吐常由预定到达率决定，保持不变的 goodput
仍需核对，作为每项实用效应推断的指标则可能产生退化的经验 bootstrap。
新的比较请求可在执行前选择 `schemaVersion: 2` 并显式声明：

```json
{
  "schemaVersion": 2,
  "metricRoles": {
    "latency": "inference",
    "goodput": "quality"
  }
}
```

其余 `http-comparison-request` 字段和阶段预算沿用原协议。当前 v2 只支持这个角色组合，
不接受任意指标删减、运行后覆盖或人工 `passed`。v1 请求拒绝 metricRoles，继续对 latency
和 goodput 全部做原有配对推断。两个版本都保留正式比较的单变量与平衡执行规则。

```text
全部题目 × 负载 × 两项原始指标
                  │
          独立核验原始记录与运行质量
                  ├─ latency: inference → 完整预声明配对族 → 区间与实用效应
                  └─ goodput: quality   → 每臂数值、计数、下限与缺项 → 描述与质量门
```

## 冻结与重建

metricRoles、所有 metricBindings、samplingBounds、推断族和每项最小观察数一起进入原
request/content/plan 锁。重建按原请求推导，重封普通文件摘要不能替换角色或删掉题目。
v2 的 metricBindings 增加 `role` 和 `minimumObservations`；仍保存全部 case × load ×
latency/goodput 绑定，samplingBounds 也完整保留。`analysisPolicy.metrics` 只包含已声明
的 inference 绑定；bootstrap 的族声明数、尾部精度和工作量依据这个前瞻冻结的族计算。

通用 `paired-analysis-policy` 与 `normalized-paired-observations` 保持原 v1 协议。
送入其核心的 values 只含 inference 指标；HTTP verification.rows.values 保存全部已核验值，
每臂的 `metricQuality` 保存全部预声明 goodput 行。新 reportContext 明确
`comparisonSchemaVersion: 2` 与 metricRoles，normalized origin 为 `http-paired-evidence-adapter-v2`。
旧 v1 输出不增加这些角色字段。

## Goodput 保留的质量门

独立 HTTP analyzer 仍先核对全部原始输入、摘要、时间线、正确答案和请求结果。
adapter 随后从原 trace 重新计数 measurement 窗口内正确完成的请求，并核对
`correctCompletedInWindowPerSecond = correctCount / durationSeconds`。
这包括对缺失、非有限值和错计数的拒绝；原正确性、drop、运行问题和生命周期门保持生效。

v2 另将冻结的 `analysis.goodput.minimumObservations` 作为质量下限核对。
缺失行保留为 unknown，观察数不足或数值无效为 failed；任何质量问题使对应运行 invalid，
从而阻止该运行贡献有效推断。全部计划臂和题目保留，不会通过过滤失败 goodput 获得通过。
每行保留 value、observations、minimumObservations、status 与 issues；总质量门同时报告
planned/passed/failed/unknown 数量。`analysis.goodput` 的原相对/绝对实用预算仍作为原请求字段
被严格保存，但在 v2 不生成 goodput 效应区间，也不作为 quality 数值波动阈值。

HTML 分开展示延迟推断与 goodput 质量/描述。常量 goodput 可以在其计数与样本数有效时通过
质量检查，同时不产生 goodput 的 degenerate 推断 cell。延迟自身的 degenerate、低样本或
不确定性仍按原规则保留；本增量没有放宽它们。

## 使用边界与入口

沿用公共命令，无新执行器：

```text
hgbench http comparison plan REQUEST --http-plan FROZEN_HTTP_PLAN --out NEW_PLAN
hgbench http comparison verify NEW_PLAN
hgbench http comparison prepare ...
hgbench task run ...
hgbench http comparison report EXECUTION --plan NEW_PLAN --out NEW_REPORT
```

先用 `http comparison describe` 或独立 planner 的 `--help` 查看版本与严格角色。
role 选择只适用于新 source、新 plan 和其后真实执行，不能给旧 004/quick 的 goodput
事后换角色。旧证据与报告继续使用原冻结 CLI 重建。校准报告里的 qualified metric 指
对应子实验冻结的 analysisPolicy.metrics；新 v2 子实验因此只对 latency 推断族判断实用变化，
同时仍要求完整 goodput 质量门。原 calibration 政策、定义字段及验收结果没有被本增量改写。

固定低到达率的 latency 推断与 goodput 描述不等于目标吞吐容量。正式吞吐主结果继续使用
前瞻冻结的吞吐—延迟曲线与独立确认点；本协议不改变该主线，也不自动接受工程或性能结论。
协议单测和合成计数样本只验证工具行为，不能作为真实 A/A、开销或余量校准。
