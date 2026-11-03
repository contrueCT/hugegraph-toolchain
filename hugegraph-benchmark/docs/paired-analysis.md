# 配对统计组件

`components/paired/` 实现原设计 §10 的区组配对效应分析，并提供独立的数值模型检查。TS CLI 通过通用 `task` 调度它；统计计算、证据封存和离线报告由 Node 组件完成。

当前输出统一为 **未校准的统计诊断**。`performanceAccepted` 和 `inferenceAccepted` 固定为 false，自动门禁禁用。真实 HTTP/LDBC 原始证据到规范输入的核验适配、前瞻性配对实验编排、真实 A/A、已知产品退化和实际影响预算校准仍待完成。旧的固定顺序 HTTP 运行不能事后改成预注册随机区组。

```text
预声明主要指标、条件、N、顺序 seed、预算
                    |
     按区组收集 A/B 独立运行及封存证据
                    |
    [原始证据核验适配：当前仍需实现]
                    |
      normalized-paired-observations
                    |
        task -> paired/workflow.mjs
                    |
     配对效应 + 区间 + 质量问题 + JSON/HTML
                    |
        模型检查 / 真实校准 / 独立确认
```

## 使用

示例输入是明确标记的数学模拟数据，只验证组件；不访问数据库，也不代表小图试跑或 SF100 结果。

```bash
npm run build
node components/paired/analyzer.mjs --help
node components/paired/analyzer.mjs --describe
node dist/cli.js task plan examples/paired-task.json --out paired-task-plan.json
node dist/cli.js --root work task run --plan paired-task-plan.json --detach --request-id paired-example-001
node dist/cli.js --root work task status --request-id paired-example-001
```

用返回的 task ID 执行 `task wait`、`task logs`、`task artifacts`。报告位于该任务目录的 `output/`。task 的 completed 仅表示组件退出 0，分析中的质量问题和未校准状态须另行读取。

独立使用或复算：

```bash
node components/paired/analyzer.mjs --input examples/paired-observations.json --output new-analysis
node components/paired/analyzer.mjs --input new-analysis/input.json --output rebuilt-analysis
node components/paired/model-check.mjs --config examples/paired-model-check.json --output new-model-check
```

输出目录必须不存在。已有失败目录保留，不覆盖或续接测量。输出包含原始输入、分析 JSON、可过滤的离线 HTML、实现摘要和文件清单。相同输入与实现产生相同分析/HTML；运行时间、主机和进程身份在 task 证据中保留。
JSON 的 `dataOrigin` 和页面同时展示输入来源；示例明确显示数学模拟，未提供来源时保留未声明/外部证据未核验状态。

## 输入契约

完整示例见 `examples/paired-observations.json`，算法固定为 `paired-median-percentile-bootstrap-v1`。

| 层级 | 字段及含义 |
| --- | --- |
| 顶层 | `schemaVersion=1`、`kind=normalized-paired-observations`、`policy`、`blocks` |
| policy | `kind=paired-analysis-policy`、`id`、`algorithm`；固定 `blockIds`（2–1000）与 `metrics`（1–128） |
| metric | `id`、`unit`、`better=lower/higher`、非负 `relativeBudget` / `absoluteBudget`、正整数 `minimumObservations` |
| 样本/精度 | `minimumPairs`、`familyAlpha`（0.001–0.2）、`bootstrapReplicates`（1000–1000000）、`minimumTailDraws`（20–1000） |
| 随机化 | `orderSeed` 分配数量平衡的 AB/BA 顺序；`resampleSeed` 固定整对抽样序列。seed 长度 1–128 |
| 身份/条件 | `baselineDigest`、`candidateDigest` 与每个区组的 `contexts[id]`，均为 SHA-256 |
| 预算 | `maxWork` 限制 N × 重采样次数 × 指标数（上限 10 亿）；`maxMilliseconds` 上限 300000 |
| block | `id`、实际 `observedOrder=AB/BA`、`baseline` 与 `candidate` |
| 每轮 run | `runId`、`evidenceSha256`、`variantDigest`、`conditionsDigest`、`status`、`values[metricId]={value,observations}` |

`status` 取 ok、invalid、correctness-failed、product-failed。`value` 必须是有限非负数，基线不能为零；observations 为该轮对应指标的原始观测量。P99 的样本下限由冻结 policy 给定，组件不把多轮观测数相加来弥补某轮不足。

这些摘要和 status 当前由调用者提供。组件验证身份一致性，**尚不自行打开或证明外部运行证据**；`gates.rawEvidence` 明确记录此缺项。规范化输入要在测量前绑定计划，事后填写相同 seed 不能证明顺序已预注册。

重复物理运行引用、重复/未声明区组或指标直接报错。缺区组、运行失败、执行顺序不符、条件或版本身份不符会抑制全族区间。单项观测缺失、数量不足、零基线和数值溢出会抑制该项，保留原始有效对数及原因；不删掉坏样本后重新计算剩余样本的结论。

## 统计语义

对延迟类计算每对 `r=B/A−1`、`d=B−A`；吞吐类取其相反数。正值统一表示退化，分别报告 r 和 d 的中位数。附各轮原值与各臂中位数，后者不称为合并的 P99。

每次有放回抽取 N 个完整区组，同一抽样索引用于该区组的 A/B、相对/绝对效应及所有主要指标。排序后的效应中位数分布使用 nearest-rank 百分位边界，保存算法、seed、B 与分布摘要。该配对方式与 [SciPy 的 paired bootstrap 定义](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html)一致；当前 percentile 方法仅为待校准候选。SciPy 默认 BCa，并指出 percentile 在实践中较少使用，本实现没有将简单 percentile 描述为已验证的优选方法。

M 个主要指标同时报告相对、绝对两类区间，共 2M 个声明。每项区间名义置信水平 `1−α/(2M)`，每尾概率 `α/(4M)`。失败指标保留其错误预算份额。B × 每尾概率必须达到 `minimumTailDraws`，这只筛查计算精度，不能保证覆盖率。Bonferroni 的族覆盖结论依赖各项真实边际覆盖达到分配水平；它不能修复 bootstrap 本身的低覆盖。[NIST 的 Bonferroni 不等式及区间说明](https://www.itl.nist.gov/div898/handbook/prc/section4/prc473.htm)。

`budgetRelation` 是数值关系：相对和绝对下界均超过正预算为 above-both-budgets；两者上界均低于负预算为 below-both-budgets。任意一类完整区间落入对应正负预算时，标记 within-at-least-one-budget，因为实际影响要求同时超过两个下限。其他情况为 crosses-budget。**这些值均不等于正式 regressed/improved/equivalent 判定。**

样本数不足、重采样尾精度不足时不产生 bootstrap 区间。效应完全相同时保留经验分布，但标记退化分布并不评估预算关系；零宽度不能证明没有不确定性。lag-1 相关只作描述，不据此接受区组独立性。

另外给出独立推导的总体中位数顺序统计量参考区间：选择满足二项分布尾概率预算的最大 k，区间为 `[X(k),X(N−k+1)]`。样本不足以产生有限区间时保留无界状态，JSON 的 null 不代表零。其覆盖下界以 IID 假设为条件，不能给相关区组作保证，也不替代原计划中的 bootstrap 校准。

## P0 数学模型检查

模型包括独立正态、独立 t(3) 重尾、AR(1) 正态（系数 0.8，故意违反独立区组假设）。两个主要指标共享相关扰动，已知总体相对中位数分别为 0 与 +20%；相对效应下方截在 −0.9 以保持观测量为正，该截断保持这些中位数。t(3) 缩放到单位方差。

冻结配置包含 seed、trials、pairs、replicates、总截止时间。每个模型/效应组合保留每次输入摘要、区间、是否覆盖已知真值和预算关系；报告族覆盖率及 95% Wilson 蒙特卡洛区间。比较 bootstrap 与上述顺序统计量参考，有助于识别算法错误和假设失效。有限模型不能证明所有真实工作负载的覆盖，输出 `calibrationAccepted=false`。实际数据库 A/A、产品退化注入、饱和/驱动余量与采集成本仍属于阶段一验收。

## 资源与失败

分析输入最多 8 MiB，输出负载最多 64 MiB；模型配置最多 16 KiB、结果最多 32 MiB。task 控制时间、日志与进程组；Linux 运行还需外部 cgroup 约束 CPU/内存。预算耗尽、输入非法、已有输出目录返回结构化错误和退出码 6，保留失败任务证据。分析不访问网络、数据库或修改测量结果。
