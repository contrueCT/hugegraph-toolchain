# 校准 v2：工程验收与误报率认证

用户已选择工程验收，真实误报率认证作为可选强度。新的校准请求使用 `schemaVersion: 2`，
在原 `http-calibration-request` 的所有字段之外增加：

```json
{
  "acceptancePolicy": {
    "id": "engineering-v1",
    "empiricalFalsePositiveCertification": "optional"
  }
}
```

另一个合法值为 `required`，表示此新计划还要求独立的实用变化误判率认证。
该选择、原 `assessment` 的 confidence/maximumFamilyFalsePositiveRate/minimumExperiments、
全部子实验、比较族和阈值一起进入 request/plan/content 锁。只能在新计划中选择，不能运行后切换。
未知字段、人工 `passed` 或缺少政策字段都拒绝。v1 请求不能携带 acceptancePolicy。

## 两条独立结论轴

```text
原始证据独立重算 → 全部预声明 A/A 实验
                       ├─ equivalent / change / uncertain → 工程 repeatability-aa 门
                       └─ family误判计数、完整分母、二项区间 → empiricalCertification

工程 A/A 门 + 其他已验证工程门 → engineering.accepted
engineering + 计划中是否要求认证 → policyAccepted
```

v2 report 同时保留完整原 `experiments`、`cohort`、issues、warnings 和缺项，再增加：

| 字段 | 含义 |
| --- | --- |
| `engineering` | 所有工程门及 accepted/status；本组件目前只接入 A/A 贡献 |
| `empiricalCertification` | required、accepted、原 rateAssessment、计数与区间；仍依赖二项模型假设 |
| `policyAccepted` | 工程门全部通过，且 required 时认证也通过；没有复用旧资格旗标 |
| `policyBlockingReasons` | 未通过的工程门；required 且认证未通过时另列认证阻塞 |

`optional` 时认证不足、uncertain 或未达界不阻塞工程轴，也不被标为已认证。
`required` 时它另行阻塞 policyAccepted；工程轴的结果保持相同。误判定义仍是原族中错误宣称
有意义改善/退化，不能改称对所有零效应检验的 nominal alpha 认证；工具也不自动证明实验独立同分布。
本政策不会自动增加实验或派发更多负载。

## 当前实现的工程边界

A/A 门只使用独立重算结果，核对全部预声明 experiment 身份，禁止复制结果凑分母。
`engineering-v1` 固定要求至少两个独立 experiment；派生 plan 的
`engineeringRequirements.repeatabilityAaMinimumExperiments: 2` 进入内容锁与派生复算。
该下限不接受请求覆盖，与只控制误报率认证的 `assessment.minimumExperiments` 相互独立；
报告 A/A 门同时给出 `minimumExperiments`、`plannedExperiments`，单个 equivalent 实验仍为 unknown。
满足固定重复数、全部实验证据和统计资格有效、没有 issues 且均 equivalent-within-declared-budgets，才通过该贡献；
已合格的 meaningful false positive 使该门 failed。uncertain、degenerate、缺失、损坏或无效证据为 unknown。
可选认证不使上述 unknown 自动通过。

主cohort报告的其余六个必需门仍显式 unknown：独立干净测量窗口、driver 余量、observer/validator 成本、
已知退化检出、warmup 稳定性、正式参数冻结。新[工程证据汇总入口](http-engineering-acceptance-report.md)在单独报告中接入原始证据reader，保留原cohort报告。known002真实限定正控制已passed；headroom002已完成独立核验，驱动与稳定性失败及两臂缺项保留；observer003的三题条件成本已有效测量，其余八题未知。总体工程验收尚未通过，缺项不能手填 passed。
所以即使合成数学样例的 A/A 门或误判率区间通过，整体 engineering.accepted/policyAccepted 仍为 false。
未来只有接入匹配题集、负载、资源、构建和观测栈的真实证据后，才能改变对应门。

004 的原质量 invalid、缺项以及 quick 的三题/两配对 screener 范围保持原样；
单元测试、功能联验、collector 原始数据和异步 trace 修复都不能代替其余真实工程校准。
吞吐/SLO范围的设计选择单独处理，本增量没有更改其指标、题目或正式 A/B 的单变量规则。

## 兼容与公开入口

沿用 `http calibration plan/verify/report`，无新执行生命周期。`http calibration describe`
及独立组件 `--help` 说明 v1/v2。request/derived plan/report 的 schemaVersion 跟随请求；
通用内容锁与 execution-set 格式保持 v1，内容摘要照常绑定新增政策。

v1 仍使用原始字段、计数、rateAssessment 和输出语义，不添加上述 v2 字段。
旧004/quick的源码、输入、证据与报告不修改；原冻结实验继续使用各自冻结 CLI 重建。
`qualificationAccepted`、`performanceAccepted` 保持旧含义与 false，本组件不把工程校准等同目标性能获准。

专项覆盖严格版本/字段、policy篡改及重封派生漂移、optional/required分离、equivalent/change/uncertain/
degenerate/missing状态、认证模型通过仍不解除其他缺项，以及真实公开CLI的v2缺项报告。
合成样例只验证协议和计算，不构成真实校准通过。
