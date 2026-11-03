# HTTP 离线报告与多组对比

`http report` 重建单次结果；`http compare` 将已有证据组织成多组、多个轮次的描述性报告。
两者共用原始记录校验、指标定义和 SVG/HTML 呈现，不向目标服务发送请求。
CLI 保持 TypeScript，报告组件使用现有 Node.js；没有新增运行时依赖、前端框架或在线资源。

```sh
# Node >=22.14；安装包提供 hgbench 命令，源码树可用 node dist/cli.js。
hgbench http report ./run --out ./reports/single
hgbench http compare \
  --input base=./r1-base --input base=./r2-base \
  --input candidate=./r1-new --input candidate=./r2-new \
  --baseline base --out ./reports/comparison

hgbench http compare --manifest compare.json --dry-run
hgbench http compare --manifest compare.json --windows --window-ms 1000 --out ./reports/detail
hgbench version
```

输出父目录会自动创建；最终输出目录必须不存在。使用新目录重建报告，原始证据保持不变。
输入支持 `raw.lock.json` 所在目录，或含 `evidence/`、`output/` 的运行根目录。多个候选同时存在时明确拒绝。
`--dry-run` 也完整验证摘要、答案、计数和时序，但不创建文件。单独的旧 `report.json` 无法替代原始证据。

## 清单

```json
{
  "title": "HTTP 服务栈比较",
  "baseline": "base",
  "groups": [
    {"id":"base", "label":"Grizzly 基线", "runs":[
      {"id":"r1-base", "path":"./r1-base", "round":"r1"},
      {"id":"r2-base", "path":"./r2-base", "round":"r2"}
    ]},
    {"id":"new", "label":"候选构建", "runs":[
      {"id":"r1-new", "path":"./r1-new", "round":"r1"}
    ]}
  ],
  "changedFactors": ["target.build"],
  "eventsFile": "./events.json"
}
```

清单内路径相对清单文件；命令行路径相对当前目录。组 ID 与运行 ID 使用 1–64 个 ASCII 字母、数字、`.`、`_`、`-`，首字符为字母或数字；标签支持中文。运行 ID 全局唯一；相同 raw 摘要不能重复计入。直接使用 `--input` 时，输入 ID 按顺序为 `input-1`、`input-2` 等。

`round` 和 `changedFactors` 是操作者声明；它们不证明 JVM 独立、随机化、资源隔离或受控因果关系。
实际请求/答案规则、负载参数、编码、窗口、超时、执行器身份等不同的同名单元会拆开，缺项留空。
报告显示完整输入资格与目标环境描述。配置相符也只表示测量口径能够并排描述。

## 图表与单图导出

```text
report.html       离线交互页；场景、负载、指标、基线筛选
report.json       多组视图；单次报告继续保留原 analysis 字段并添加报告身份
inputs.json       原始锁摘要、组/轮次、目标和资格
summary.csv       各单元/组的运行级中位数、最小值、最大值和缺项
runs.csv          全部逐次指标与计数、质量及事件
charts/*.svg      每张绝对值图、基线变化图、曲线、可选时间窗图
```

每张图右上角有 **导出此图 SVG**。导出的 SVG 自带坐标、单位、标签和颜色，可以单独插入文档；只复制 HTML 到另一台电脑，仍能通过按钮导出每张图。CSV/JSON 的下载链接需要同目录文件。没有 PNG 转换服务，SVG 可以继续通过浏览器或文档软件导出为位图。

默认指标为正确完成吞吐。横柱从零开始，点保留每次运行，范围线是观察到的最小–最大值；它不表示置信区间。变化图为 `(候选中位数 / 基线中位数 - 1) × 100%`，基线为零、缺失或样本不足时显示 N/A。不同轮次的请求不合并计算分位数。

页面持续显示正确/发送、错误、未发送和迟到计数。错误率为 `failed/sent`，未发送比例为 `dropped/scheduled`，late 可与其它计数重叠。P99 少于 1000 个正确样本时在主图留空，原始观察值在明细保留；达到门槛也不表示尾延迟已获统计资格。

多个负载点提供吞吐—P50 观察曲线，只连接已测点；固定到达率的吞吐不能推出峰值容量。此版本不计算总分、统计显著性或赢家，`performanceAccepted=false` 始终保留。

## 诊断与事件

明细按预热/测量阶段展示 HTTP 状态码、outcome 分布、调度原因、驱动 CPU/节流/RSS，以及最多 8 条异常 trace 定位。
阶段结束边界漏发、超过最大启动延迟分别计数，原因可能重叠；信息不足显示原因未定。原始质量标记不删除。某次边界漏发不能直接解释为持续驱动过载。

`--windows` 复用现有 phase-windows 逻辑，在完整校验后重读 trace。默认 1 秒窗口；P50/P95 每窗至少 100 个正确样本，P99 另需 1000 个。样本不足的点留空；固定预热不证明稳态。窗口归属按 sentMs/observedMs，跨窗完成与 drain 沿用原合约，见 [时间窗定义](http-phase-windows.md)。自动运行的普通报告不额外启用分窗读取。

事件文件是最多 256 条对象的 JSON 数组：

```json
[
  {
    "inputId": "r1-base",
    "kind": "GC",
    "runId": "r0001",
    "caseId": "scalar-add",
    "loadId": "open-100",
    "startUtc": "2026-10-24T16:59:02.113Z",
    "endUtc": "2026-10-24T16:59:02.319Z",
    "source": "gc.log / GC(65)",
    "message": "Full GC 205.792 ms；UTC 对应仅用于诊断"
  }
]
```

必须有 inputId、message、source，其余字段可省略。时间用 UTC 的 `Z` 后缀；没有时间时按输入/选择器作备注。时间对应使用单次运行的 UTC 开始/结束，包含预热和 drain；没有锚点时按输入级保留。事件不自动剔除样本，不证明因果；原始日志解析和持续采集继续使用专项工具。清单的 `eventsFile` 和命令行 `--events` 二选一。

正式 HG 测试仍需关闭宿主 swap 并控制内存监控触发的手动 GC。关闭 `MemoryMonitor` 后，`LoadDetectFilter` 等其他调用路径仍可能执行 `System.gc()`；应记录 JVM 策略和实际 GC 事件，不能凭一个配置值认定全程无显式 GC。报告重建不修改 JVM 策略，也不为历史数据补发性能资格。

## 边界与错误

2–8 组、最多 32 份原始输入，顺序离线处理；JSON 清单/事件各不超过 1 MiB。
报告最多 512 单元、8192 次运行、32768 个窗口和约 4096 张图，组合输出上限 64 MiB。
CLI 组件期限 300 秒，超出限制时拆分报告。原始 analyzer/window 组件各自的更小限制仍适用。

常用错误：`REPORT_INPUT`（清单/参数）、`REPORT_EVIDENCE`（指出原始输入和校验原因）、
`COMPARE_DUPLICATE_RAW`（重复证据）、`REPORT_OUTPUT_EXISTS`（新建输出路径）、
`REPORT_OUTPUT_PATH`（父路径/权限）、`REPORT_TOO_LARGE`（拆分）、`REPORT_COMPONENT`（组件失败/超时）。
输出仍为既有结构化 JSON；`--help` 不读取原始数据或连接目标。

`--version` 保留 npm 版本；`version` 命令返回内容 buildId、报告/原始协议版本和 development/release 状态。
打包前记录源码、编译输出与依赖锁摘要，安装位置不参与身份。修改代码后原 stamp 失配，显示 development。
旧 raw 可被新 reader 校验；新版呈现不与旧版 HTML 保证字节一致。原冻结包仍用于复现旧版报告。
