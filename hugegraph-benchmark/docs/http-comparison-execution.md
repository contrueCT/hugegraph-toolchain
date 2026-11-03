# HTTP 前瞻性配对计划

`components/http/comparison-plan.mjs` 将一份版本化 HTTP profile 与共同的构建、环境和外围配置证据冻结为 A/A 或 forward/embedded 的配对计划。组件只读本地输入、创建新计划目录；不启动 Docker、不访问 HTTP、不执行分析或测量。

这一步补充 M2 的前瞻性计划能力。受控启停、模式探针、真实执行台账、原始证据桥接及校准仍由相应组件和控制器完成。计划中的 `executed`、`performanceAccepted`、`inferenceAccepted` 固定为 false；当前只接受 `functional` / `calibration` 用途。

```text
版本化 HTTP 题集/profile + 共同身份文件 + 显式区组/seed/阈值/预算
                         |
                  comparison-plan freeze
                         |
       planDigest + 完整内容锁 + AB/BA 顺序 + 每臂唯一 runId
                         |
               verify -> 控制器先持久化计划绑定
                         |
     每臂新进程 -> 实际身份/模式 -> 预热/测量 -> 排空/后检 -> 封存
                         |
          执行台账 + 原始 HTTP 证据 -> 配对桥接 -> paired analyzer
```

## 入口与产物

先执行 `npm run build`，使共享的 HTTP profile schema 可用。使用已有 `hgbench http plan` 的目录；组件读取其中的 `profile.json`、`benchmark/` 和 `target-evidence.json`，复制并校验其内容。原 task 不会被执行。

```bash
node dist/cli.js http comparison describe
node dist/cli.js http comparison plan comparison-request.json --http-plan http-plan --out comparison-plan
node dist/cli.js http comparison verify comparison-plan

# 独立组件入口，与上述薄 CLI 委托相同
node components/http/comparison-plan.mjs --help
node components/http/comparison-plan.mjs --describe
node components/http/comparison-plan.mjs \
  --input comparison-request.json --http-plan http-plan --out comparison-plan
node components/http/comparison-plan.mjs --verify comparison-plan
```

`--out` 必须不存在，父目录须存在。失败后保留部分目录，使用新的实验 ID 与新目录重新冻结。计划目录保持只读；实际执行目录和台账放在计划目录之外。验证会拒绝额外文件、遗漏、符号链接、路径穿越、内容变化和改变后的语义，即使修改者仅更新了文件摘要。

| 文件                                 | 内容                                                                         |
| ------------------------------------ | ---------------------------------------------------------------------------- |
| `plan.json`                          | `planDigest`、共同条件、固定顺序、每臂身份/预算、完整主要指标族和统计 policy |
| `plan.lock.json`                     | 每个产物的路径/大小/SHA-256、`contentDigest`，包含原始输入快照               |
| `request.json`                       | 规范化后的实验请求，身份路径指向计划内快照                                   |
| `inputs/`                            | 原 profile、原目标证据清单、共同 build/environment/settings JSON 的原始字节  |
| `benchmark/`                         | 已验证的题集、参数/答案、协议、scoring/calibration/release 和 bundle lock    |
| `target/`                            | 原 HTTP 计划的目标证据附件快照                                               |
| `target-files.json`                  | 原目标附件和三份共同身份文件的内容清单                                       |
| `profiles/A.json`、`profiles/B.json` | 两臂共用的 profile 模板；仅 `target.mode` 不同，A/A 时完全相同               |
| `implementation.json`                | 计划器、配对核心、题集检查器、共享 schema、package lock 的摘要               |

`verifyComparison(directory)` 校验所有内容锁，并依据冻结输入重新派生顺序、身份、预算、profile 与 policy。它还核对当前计划器/schema 与 `implementation.json`。计划可搬迁，验证不再打开原始输入位置。依赖安装及目标运行时的实际内容检查由控制器完成。

CLI 成功输出一个 JSON 对象；退出 0 仅表示创建/完整性校验成功。错误在 stderr 输出 `{ok:false,error:{code,message,retryable:false,nextAction}}`，退出 6。`--describe` 返回能力、用途和预算边界。JavaScript 调用入口为：

```javascript
import { freezeComparison, verifyComparison } from "./components/http/comparison-plan.mjs";
await freezeComparison({ inputFile, httpPlanDirectory, outputDirectory });
await verifyComparison(outputDirectory);
```

## 请求示例

下例数值展示输入形态，是功能/校准工作的临时参数；不构成正式采样时长、重复数、性能阈值或资源配置推荐。SHA-256 必须替换为实际文件摘要。`profile.benchmark.purpose` 与请求用途一致。

```json
{
    "schemaVersion": 1,
    "kind": "http-comparison-request",
    "id": "http-pairs-calibration-001",
    "purpose": "calibration",
    "comparison": "forward-embedded",
    "modes": { "A": "forward", "B": "embedded" },
    "blockIds": ["b01", "b02", "b03", "b04"],
    "orderSeed": "http-pairs-calibration-001:order",
    "sharedEvidence": {
        "build": { "path": "build.json", "sha256": "<actual-sha256>" },
        "environment": { "path": "environment.json", "sha256": "<actual-sha256>" },
        "settings": { "path": "settings.json", "sha256": "<actual-sha256>" }
    },
    "analysis": {
        "familyAlpha": 0.05,
        "minimumPairs": 4,
        "bootstrapReplicates": 100000,
        "minimumTailDraws": 20,
        "maxWork": 1000000000,
        "maxMilliseconds": 60000,
        "resampleSeed": "http-pairs-calibration-001:resample",
        "latency": { "relativeBudget": 0.05, "absoluteBudget": 1, "minimumObservations": 1000 },
        "goodput": { "relativeBudget": 0.05, "absoluteBudget": 1, "minimumObservations": 1000 }
    },
    "budgets": {
        "prepareMs": 10000,
        "startMs": 120000,
        "modeProbeMs": 40000,
        "settleMs": 1000,
        "stopMs": 30000,
        "sealAndAnalyzeMs": 60000,
        "maxEvidenceBytesPerArm": 134217728,
        "maxTotalEvidenceBytes": 2147483648,
        "maxTotalMilliseconds": 14400000
    }
}
```

身份文件路径相对请求文件解析，叶节点必须是有界普通文件，每份最多 8 MiB。三份文件都是非秘密 JSON 对象；规划阶段检查内容摘要，并保留 `runtimeIdentityVerified=false`。运行时需确认它们对应实际候选构建和环境。

`settings.json` 应完整记录固定的 JDK、JVM 参数、backend、REST/Gremlin 线程和队列、序列化、鉴权机制、压缩、CPU/内存/PID 限制、collector 配置和缓存/数据重置方式。鉴权凭据通过 profile 的 `authorizationEnv` 名称交给执行器。共同配置中省略 `gremlinserver.rest_execution_mode`；该属性仅从每臂的声明注入。计划器检查已提供的设置及单变量 profile，控制器在运行时核验配置完整性与实际值。

A/A 设置 `comparison="aa"`，并令 `modes.A`、`modes.B` 同为 forward 或同为 embedded。两臂的 `variantDigest` 相同，但每个物理运行的 `runId` 均不同。forward/embedded 固定 A=forward、B=embedded，执行先后由 seed 决定。

## 顺序、统计单位与边界

- 显式提供 2–1000 个互异区组 ID，并使用偶数个区组。复用 `paired/core.mjs` 的 `balancedOrders` 和 SHA-256 sampler，AB/BA 数量各半，seed 固定后顺序可重建。
- 每个区组包含两个连续、独立启停的物理运行。`profile.repeats=1` 且 `serviceChecks=true`；每臂按同一 case/load 顺序运行完整已选题集。预热属于每臂的 HTTP workflow。该空图脚本题集不访问图数据，不清理 OS 缓存。
- 所有臂绑定同一 build/environment/settings、endpoint、连接/压缩/鉴权、题目/答案、负载和计时设置。每臂的 `profileFile`、`profileSha256`、`expectedMode`、`variantDigest` 与 `runId` 在计划里固定。
- 指标族为完整 `case × load × 2`，最多 128 项。`analysis.latency.quantile` 可选 `p50`、`p95` 或 `p99`；省略时严格沿用 P99。open 使用 `correctEndToEndMs.<quantile>`，closed 使用 `correctResponseMs.<quantile>`；共同吞吐指标为 `correctCompletedInWindowPerSecond`。`metricBindings` 保留逐项原始来源，不按结果删掉题目或指标。
- 延迟 observation 数始终使用所选分布的 `count`；分位数及其来源路径影响 metric ID，并由请求、计划和内容锁固定。`quantile` 不进入 paired policy 的 metric 阈值结构。goodput observation 数从已验证原始 trace 计算测量窗口内正确完成数。不要把多轮样本合并后计算分位数。
- 选择 P50/P95 不会自动降低 `minimumObservations`、独立区组数、尾精度或其他质量门槛。需要短功能检查时，应在新请求里显式预声明相应统计目标和阈值；P50 的结果不能充当 P99 的可靠性证据。已冻结计划继续使用各自冻结实现，新实现不会重释旧证据。
- `samplingBounds` 每项的 `maximumPossibleObservations` 对 open 是 `ceil(rate × duration / 1000)` 与请求 cap 限制下的上界，对 closed 仅是请求 cap，均不预测实际正确样本。`estimatedObservations` 和 `estimatedCorrectRequestsPerSecond` 保持 `null`。open 的 `minimumPotentiallyReachable=true` 只表示上界未排除达到下限；closed 在没有实测吞吐时为 `null`，如果请求 cap 已不足则为 `false`。实际正确率、驱动余量及分位数可靠性仍需执行和校准。
- policy 直接兼容现有配对分析器。计划阶段检查 minimumPairs、完整指标族尾精度和分析计算预算；仍保留所有推断/性能接受开关为 false。
- `conditionsDigest` 绑定共同配置和区组，`runId` 绑定完整计划输入、区组和 A/B 臂。改变 seed、参数或实验 ID 会产生新的运行身份。

## 预算和控制器接线

`plan.json` 以及 plan/verify 的返回值均包含 `estimate` 和 `samplingBounds`，供 CLI 和 agent 直接预览。`estimate.counts` 列出题数、负载点、case/load 组合、区组、物理臂、HTTP run 和前后答案检查次数；固定基线按实际顺序执行的所有臂计算：

```text
perArm = cases × loadPoints × (warmupMs + durationMs)
fixedBaselineMilliseconds.total = arms × perArm
includingSettleTotal = total + arms × settleMs
```

该基线表示完成整个计划所配置的固定窗口，并不预测启动、响应 drain、检查或写证据的耗时。`overheads` 对 prepare/startup、前后 mode/答案检查、drain、seal/analysis、cleanup 和可选时钟参考分别列出已声明安全 allowance；没有实测模型时 `normalMilliseconds=null`，关闭的时钟参考为 0。工作流另有调度、进程启动和整体验证余量，不能把各项 allowance 当作正常耗时。

`normalEtaMilliseconds=null`、`confidence="low"` 明确表示尚无完整正常 ETA。`safetyUpperBoundMilliseconds` 等于 `budgets.requiredTotalMilliseconds`，供执行期限预算审阅；它是保守上界，不能在界面上标成“预计通常耗时”。预览和真实资源强制限制分别处理。

每臂预算包含 prepare、start、前后两次 mode probe、settle、完整 HTTP workflow、stop、seal/analyze。HTTP workflow 时间上界沿用现有控制器的计算：各 case/load 的预热、测量、两次 drain 和调度余量，加前后答案预检，再加 60 秒工作流余量。全部臂顺序执行；全程预算还包含一次配对分析预算。

证据预算累计每臂 cap，并为计划及汇总分析保留 64 MiB。单臂 cap 必须覆盖 trace cap 与冻结元数据的计算余量。这些检查提供可审阅的预算，实际时间/磁盘/cgroup 的强制限制由外层控制器执行。

控制器后续需要完成：

1. 在第一次 dispatch 前持久化不可变的 `planDigest`，绑定新执行 ID、各 arm 的 runId 和后续 evidence/receipt。创建时间或匹配的 seed 无法独立证明测量遵循了前瞻计划。
2. 每臂创建新目标进程，检查共同构建、设置和环境，执行真实模式探针，再按固定预热和测量协议运行。前后探针都在计时窗口之外。
3. 记录实际顺序、启动/停止身份、采集配置、资源窗口、答案校验、raw lock 与报告摘要。转换/导入重叠窗口保留为功能证据，并使性能门失败。
4. 原始证据桥接按计划 `metricBindings` 生成 `normalized-paired-observations`，把 `analysisPolicy` 原样交给 paired analyzer。缺臂、失败、顺序错乱、环境变化和 mode 未验证都保留；不得静默补跑并删除旧记录。

运行任务退出成功、计划校验通过和正式性能有效性分别记录。M0 的正式参数定值、M2 的 A/A/已知退化/驱动与采集成本校准以及 M3 的实际对比仍有各自的验收条件。
