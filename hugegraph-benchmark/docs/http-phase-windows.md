# HTTP 预热与测量分窗诊断

`components/http/phase-windows.mjs` 从已有完整 HTTP raw evidence 离线重建 arrival 窗口，
展示固定预热与测量期间的请求延迟、校验耗时、错误和调度问题随时间的变化。
分析先完整调用 `analyzer.mjs`，保留其 raw lock、题集、附件、服务检查、计数、时序和预算校验。
输入中损坏或重新封存后不符合协议的记录会失败，不产生一个看似可用的趋势报告。

每个 run、每个 phase 各自从零计时；只使用原始 `performance.now()` 相对毫秒数。
输出 `performanceAccepted=false`、`warmupStable=null`、`qualification="not-assessed"`。
此组件帮助观察固定预热是否值得进一步校准，不能自动确定稳态、驱动余量或性能验收。
它不关联 JVM、collector、主机资源及其他进程的时钟。

## 使用方式

无需在线 HugeGraph、Docker、Python 或网络连接。输入目录需要包含 `raw.lock.json`，
通常是一次 workflow 的 `output`，或比较证据中某一 arm 保存的原始请求证据目录。
输出目录必须尚不存在。

```bash
hgbench http calibration windows --help
hgbench http calibration windows /absolute/path/to/raw \
  --out /absolute/path/to/new-window-report \
  --window-ms 10000 \
  --minimum-requests-per-window 100 \
  --minimum-p99-requests 1000
```

公共 CLI 只通过既有有界组件监督器调用固定的 `phase-windows.mjs`，分析算法留在独立组件。
监督截止为 330 秒，取消或超时后最多另有 5 秒强制回收预算。
成功返回标准 `schemaVersion/ok/command/data/nextActions` JSON envelope，
两个 `kind="read"` next action 分别给出 `report.json` 和 `report.html` 的绝对 `path`。
标准输出只保留简短组件结果与文件位置，完整窗口数据保存在报告中。

也可以独立调用组件：

```bash
node components/http/phase-windows.mjs --help
node components/http/phase-windows.mjs describe
node components/http/phase-windows.mjs report \
  --input /absolute/path/to/raw \
  --output /absolute/path/to/new-window-report \
  --window-ms 10000 \
  --minimum-requests-per-window 100 \
  --minimum-p99-requests 1000
```

三个设置都必须显式提供。以上为诊断示例，需要在查看数据前选择适合负载的窗口与样本门槛。
10 requests/s 的 10 秒窗口仅约 100 次到达；其 P99 输出为 `null`、状态为 `unknown`。
增加窗口会改变时间分辨率，不能通过事后调参把某条曲线解释为已通过稳定性校准。

`describe` 返回参数、上限及离线行为的 JSON。成功 `report` 写入确定性的 `report.json`
和可直接打开的 `report.html`，标准输出只返回简短 JSON。
组件错误沿用 `http-component-error` JSON 与退出码 `6`；公共 CLI 将组件失败包装成
`ok=false`、`HTTP_CALIBRATION_FAILED`（退出 `6`）并保留错误详情。
缺少选项、非十进制整数或越界的公共 CLI 参数返回 `CLI_USAGE`（退出 `2`），不创建输出。
现有输出不会覆盖。
HTML 使用内嵌数据和脚本，所有文本通过 DOM `textContent` 显示，无外部请求或依赖。

模块接口：

```js
import { analyzePhaseWindows, reportPhaseWindows } from "./components/http/phase-windows.mjs";

const policy = {
    windowMs: 10000,
    minimumRequestsPerWindow: 100,
    minimumP99Requests: 1000,
};
const diagnostics = await analyzePhaseWindows(rawDirectory, policy);
await reportPhaseWindows(rawDirectory, newOutputDirectory, policy);
```

## 一条记录只属于一个 arrival 窗口

```text
phase 相对时间       0          10s         20s       duration        elapsed
arrival ownership  [ window 0 )[ window 1 ) ... [last) [ drain arrivals ]
                   ^ sentMs / observedMs 决定唯一归属
                   |----------------------- response end
                                  延迟仍属于起始 arrival 窗口
                                              |-------- drain completion overlay
```

- 已发送请求使用 `sentMs`；未发送请求使用实际 `observedMs`。
  `scheduledMs` 只参与原始协议校验和调度延迟计算。
  例如取消时提前记录未来的计划到达，仍按实际取消观察时刻归属。
- 名义窗口是 `[startMs,endMs)`，包括无请求的窗口与末尾不足 `windowMs` 的窗口。
  到达时刻大于等于 `durationMs` 的记录归属单独的 `drain.arrivals`。
- `drain.completionOverlay` 汇总终止或校验完成时刻严格大于 `durationMs` 的记录。
  这些记录已经计入其 arrival 窗口；不能把 overlay 与 arrival 计数相加。
- 请求终止时刻、或完整响应的 `endMs + validationMs` 严格大于 arrival 窗口上界时，
  分别计入跨窗标记。刚好在上界完成不会标为跨窗。
- phase 若提前结束，所有名义窗口仍保留；`observationThroughMs`、`phaseEndedEarly`
  显示实际观察范围，后面的空窗不能证明一段完整观察。

每个 phase 的 `totals` 必须能由所有名义窗口与 `drain.arrivals` 逐项相加复算。
结果还重新核对原始 phase 的总数与 outcome 直方图：

```text
records = correct + errors + dropped
sent = completed = correct + errors
notSent = dropped
```

`errors` 包括已发送但结果非 `ok` 的请求，例如错误答案、HTTP 错误、超时。
当前 raw 协议的 `dropped` 和 `notSent` 相等，`late` 与其他项重叠；它们不能横向相加。
请求在 measurement 结束后才完成，仍完整保留在原 arrival cohort 的延迟中。
预检与后检由 analyzer 验证，通过数量写入 `verification.serviceChecks`，不混入负载窗口。

## 延迟与样本口径

每个窗口包含以下独立分布，全部使用 nearest-rank 分位数：

| 字段                    | 样本                                               |
| ----------------------- | -------------------------------------------------- |
| `correctResponseMs`     | `outcome=ok`，`endMs-sentMs`                       |
| `allCompleteResponseMs` | 收到完整响应体的所有请求，包括 HTTP 错误和错误答案 |
| `failedDurationMs`      | 已发送但非 `ok` 的终止耗时，包含超时与传输错误     |
| `schedulerDelayMs`      | 所有已发送请求的 `sentMs-scheduledMs`              |
| `validationMs`          | 完整响应的驱动端校验耗时，独立于响应计时           |

每个分布使用自己的 `count`，而非整个窗口的到达数。
P50/P95 要求 `count >= minimumRequestsPerWindow`；P99 还要求
`count >= minimumP99Requests`。不足时相应输出为 `null`，状态为 `unknown`。
`observedNearestRank` 保留可复算的原始次序统计量，其存在不会改变样本状态。
达到门槛只表示 `descriptive`，不表示独立样本、稳定尾延迟或有置信区间。

HTML 曲线展示通过样本门槛的正确响应 P50/P95，unknown 留空；表格同时保留 P99 状态、
校验耗时、错误、丢弃、迟到与跨窗完成，避免一条延迟曲线掩盖驱动问题。
原始 phase 的 CPU、event-loop 与 RSS 只列在 `driverWholePhase`，不能细分到窗口。

## 有界与重建

- `windowMs`：整数 `1..300000`。
- `minimumRequestsPerWindow`：整数 `1..100000`。
- `minimumP99Requests`：整数 `1000..100000`。
- 总窗口数（所有 run/phase，含每 phase 一个 drain arrival 桶）：最多 `16384`。
- JSON 与 HTML 合计最多 `64 MiB`；先计算并检查，再创建输出目录。
- 原始文件继续受 analyzer 的文件、trace、run、请求与行长预算约束。
  分窗再流式读取 trace，并核对实际字节数和 SHA-256；run/profile 与 raw manifest 也再次核对。
  输入应在整个分析期间保持不变。

报告不包含生成时刻或本机输入/输出目录，可复制 raw 到另一位置后逐字节离线重建。
专项测试覆盖精确分窗与 drain 守恒、边界与未来取消、独立 P99 门槛、重封后的坏时序/计数、
预算与不覆盖、CLI，以及真实本地 HTTP workflow 的两 phase 端到端路径。
合成记录明确作为协议夹具，本地 HTTP 仅证明工具功能；这些检查不替代真实 A/A 校准。
