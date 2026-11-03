# HTTP 完成时间窗口

此组件重算原 `raw.lock.json`、答案、服务探针、计数和时间线，再按 measurement
请求的 `endMs` 分组，提供吞吐曲线所需的完成时间证据。它继续保留原质量问题。
完整且可重算的零正确响应点显示 0；损坏 raw、重复或未结束请求、缺失及失败探针会拒绝。

```sh
node components/http/completion-windows.mjs \
  --raw /absolute/raw \
  --window-ms 5000 \
  --minimum-latency-observations 100 \
  --out /absolute/new-completion-windows
```

全部参数显式给出；输出新目录中的 `report.json` 和 `evidence.lock.json`，不覆盖已有目录，
不把输出写入原 raw。函数入口为
`summarizeCompletionWindows(rawDirectory, {windowMs, minimumLatencyObservations})`。
原始 HTTP profile、analyzer、runner 和旧 arrival 窗口协议均保持原有含义。

每个 measurement 窗为 `startMs,endMs)`，最后一个窗包含 `endMs == durationMs`，
与原 `correctCompletedInWindowPerSecond` 精确一致；drain 只收严格大于 duration 的事件。
同一边界规则用于发送、结束和未发送观测，避免零耗时、恰在 duration 的事件破坏守恒。
尾窗采用实际窗宽，保留不足样本；提前中断时未完整观察的窗口速率为 null。

```text
scheduledMs ── planned scheduledArrivals
sentMs / unsent observedMs ── observedArrivals = sent + notSent
sent ────────────────────── in-flight ────────── endMs
                                             finished = correct + sentErrors

backlogEnd = backlogStart + sent - finished
```

`scheduledArrivals` 是预先计划的到达；`observedArrivals` 表示实际发送或未发送处理事件。
取消可以提前标记未来计划，因此两者分别保留。这里的 backlog 是已发送但尚无终态的请求数，
不表示目标内部队列。发送请求的 timeout、transport error 也属于 `finished`；完整 HTTP
响应单独计为 `completeResponses`。未发送请求没有完成时间，按 `observedMs` 计入
`notSent`，保留 `scheduler-late`、`concurrency-limit`、取消等原始分类。
错误、timeout 与 drain-timeout 是可交叉查询的计数，不能相加后当成互斥总数。

每窗 P50/P95/P99 分布只包含该窗实际结束的 measurement 请求。正确 response/end-to-end、
全部完整响应以及失败持续时间各有独立 count。P50/P95 要求显式的
`minimumLatencyObservations`；P99 要求 `max(1000, minimumLatencyObservations)`。
不足时正式分位为 null、状态 unknown；原近似秩值保留为描述性观测，不合并窗口凑样本。
完成窗的 count/速率与旧 arrival cohort 的 count/速率含义不同。

来源记录 raw manifest SHA、当前组件及依赖源码 SHA 和组件算法版本；重建不嵌入输入、输出绝对路径，
相同工具与 Node 下移动 raw 后可逐字节复算。数据上限为输入 512 MiB、trace 总计 256 MiB、
50 万记录、2 万窗；manifest/单份 JSON 8 MiB，输出报告 32 MiB。函数有 300 秒协作截止，
独立 CLI 另有 300 秒 watchdog；调用方仍可使用既有有界进程监督。

这些窗口提供描述性稳定性证据。`warmupStable=null`、`engineeringAccepted=false`、
`performanceAccepted=false` 固定保留，不自动证明稳态、服务端饱和、驱动余量或性能资格。
driver 数值继续表示整个 phase（含 drain），没有逐窗资源、JVM 或跨进程时钟归因。

专项：`node --test tests-normative/http-completion-windows.test.mjs`。
合成 trace 明确标记为协议夹具；另有一次短的本机 HTTP 工具验证，不构成性能测量结论。

## 公开 CLI 与真实原始证据验证

```bash
hgbench http capacity windows /absolute/sealed-raw \
  --out /absolute/new-completion-report \
  --window-ms 5000 --minimum-latency-observations 50
```

TS 入口只负责参数校验、330秒监督及组件结果协议，计算仍在独立组件。
`--help`列明完成与到达口径、错误保留及P99最低样本。
[真实原始记录离线验证（本机记录未随包分发）覆盖三题、450个测量请求、9个完成窗；
独立Python计数重算及11份组件依赖迁移后的报告逐字节重建通过。没有重发请求，未据此认定稳态或容量。
