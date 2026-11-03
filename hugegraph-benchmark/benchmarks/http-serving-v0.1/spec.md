# HG-Bench HTTP Serving v0.1

版本：`0.1.0-draft.1`，本地规范候选。独立题集、协议、答案、成绩规则、校准计划和发布条件由 `bundle.lock.json` 锁定。真实全题集校验、校准后的正式 profile 和 A/B 尚未完成；`performanceAccepted=false`。

## 题集、确定性答案与覆盖

规范源为 `catalog.json`，每题固定 ID、请求体、bindings、形态及独立 oracle。`answers.json` 冻结完整 `result.data` 的规范化 SHA-256、JSON 字节数和行数。`reference.mjs` 用独立 JavaScript 运算构造答案，不执行 Groovy、不访问数据库；Python 专项用另一实现复核内容摘要。旧 `examples/http-gremlin.json` 保留为自定义工具例子，不再作为正式题集定义。

| 题目 | 固定参数 / 结果 | 测量范围 |
| --- | --- | --- |
| scalar-add | 17 + 25 → [42] | 小整数脚本与响应固定开销 |
| structured-one | x=7；一行 map，含布尔、字符串与整数数组 | 结构化单结果的序列化 |
| rows-0016 / 0256 / 4096 | 有序 i=0…n−1；每行 id/name/value=3i/active | 多行序列化、完整结果与顺序 |
| string-0001k / 0016k / 0064k / 0256k / 1024k | 一行字符串，分别 1/16/64/256/1024 KiB ASCII x | 单大值响应与缓冲/尾延迟 |
| string-unicode | 固定汉字、emoji、引号、反斜杠、换行文本重复 512 次 | UTF-8 与 JSON 转义一致性 |

每题要求 HTTP 200、`status.code=200` 和完整有序 `result.data`。JSON 对象键按 UTF-16 键序递归排序后使用紧凑 JSON/UTF-8；数组顺序、重复项、数值/字符串/布尔/null 类型都保留。校验器对收到的整个对应 JSON 值计算 SHA-256，比对冻结摘要，缺失字段失败。小整数域不超过 2^53−1，不支持以普通 JSON 数值验证大图长整型 ID。响应 envelope 的 requestId、非语义 message/attributes/meta 不进入答案；完整 wire body 另有每请求摘要。

单字符串的标称 KiB 表示字符串内容 UTF-8 字节数；`resultDataCanonicalBytes` 包含 JSON 数组/引号/转义；实际 wire/decoded body 包含完整服务 envelope，必须从真实响应记录。多行题记录精确行数和规范化字节数，不把行数叫作响应大小。x 字符串高度可压缩；gzip 结果只适用于这类压缩特征。Unicode 题覆盖转义语义，不是生产文本分布。

这些脚本不读写图，仅用于 HTTP 服务路径、脚本运行、JSON 序列化和传输。固定小图点查/遍历以后采用新题集版本；大图继续由 SF100/HStore 验收。

## 可执行规范与实验设置

```bash
node dist/cli.js http suite
node dist/cli.js http suite --id string-1024k
node dist/cli.js schema http-suite-settings
node dist/cli.js http generate examples/http-suite-functional.json --out /tmp/http-profile.json --dry-run
node dist/cli.js http generate examples/http-suite-functional.json --out /tmp/http-profile.json
node dist/cli.js http plan /tmp/http-profile.json --out /tmp/http-attempt-001
```

生成 settings 显式给出 endpoint、目标证据、caseIds、开放/闭环负载、warmup/duration/repeats、timeout/drain 和各项预算。生成器不推断正式参数。输出 profile 绑定版本、bundle SHA、完整或子集覆盖及 functional/calibration 用途；新文件独占创建，dry-run 不写文件、不访问目标。未知题目、改写请求/答案、身份或内容锁不符、响应预算不足均拒绝。

plan 将完整小型规范包复制进新 attempt 并纳入 task 内容锁；workflow 原样复制到原始证据。分析器只读锁定的数据文件验证题目绑定，不执行证据里的 reference 程序。这样离线重建依然能核对原版本的题集、期望答案和覆盖。自定义 profile 没有 benchmark 绑定，不能据此宣称标准题集成绩。

`protocol.json` 固定 HTTP/1.1 POST /gremlin、keep-alive、无管线化/重试/重定向、identity/gzip 和匿名/鉴权的独立分组。服务端线程池、队列、序列化/JVM/资源边界等由实际部署证据冻结；实际模式需独立核验。每个 A/B arm 采用独立进程、相同初始化和预热协议，清楚区分进程/编译缓存与图缓存。全题集包括所有 11 题；子集报告明确列出 coverage，不能输出全题集总分。

`calibration-plan.json` 逐项定义计时记账、全题集真实校验、预热稳定性、driver 余量、观察成本、A/A、已知退化、负载点和干净窗口的证据要求。warmup、duration、pairs、负载点、最小样本数、差异阈值和环境预算在真实校准后固定，目前为 null。示例 settings 只验证功能，不能当成正式测量参数。

`scoring-policy.json` 预先声明开放模型的端到端 P99/正确完成吞吐，闭环模型的响应 P99/正确完成吞吐为主指标，错误/丢弃和资源指标同时披露。不合成一个总分。场景×负载×主指标比较族、实际阈值、独立配对数与种子必须先于正式 A/B 冻结；保留全部场景、缺项和无效原因。

## 负载、计时与计数

```text
固定 profile → task 身份校验 → 独立 runner 子进程
                                   |
                warmup → 排空 → measurement → 排空
                                   |
                逐请求 NDJSON + 运行摘要 + SHA 清单
                                   |
                独立 analyzer → report.json / report.html
```

- 开放模型：第 i 个到达时刻固定为 `i * 1000 / ratePerSecond` 毫秒，窗口内总数为 `ceil(durationMs * ratePerSecond / 1000)`。按单调时钟调度，迟到超过 maxStartDelayMs 或已过测量窗口时记 scheduler-late，达到 maxInFlight 时记 concurrency-limit。无隐藏等待队列、重试或无限补发。
- 闭环模型：每个并发 worker 在上个请求及校验/证据写入结束后发下一请求；达到窗口截止时停止新请求。maxRequestsPerPhase 提前触发会标记预算耗尽，不能视作完整测量。
- 记录 scheduledMs、sentMs、socketMs、endMs；sent 表示调用 HTTP request API，包含其后 socket/DNS/TLS/网络等待，并不声明网络字节已经发送。时间原点为各 phase 的 performance.now。
- 完整响应延迟是 endMs-sentMs；开放模型端到端延迟是 endMs-scheduledMs。在完整 body 的 end 回调先记录 endMs，再做 SHA、gzip 解压、JSON 解析和答案校验。validationMs 包含解压，decodeMs 为其中单独的解压成本。同步证据写入另计，但二者均消耗驱动资源并可能影响后续请求。
- 请求超时从调用 request API 计算，覆盖连接和完整 body；到窗口结束+drainMs 仍未完成的请求记 drain-timeout。丢弃没有虚构延迟。错误/超时耗时与正确响应分位数分开。
- 每 phase 要求 scheduled=sent+dropped，sent=completed；序号连续且唯一。warmup 也校验答案、错误和丢弃；只将 measurement 用于报告延迟/吞吐。
- 默认 HTTP/1.1 keepalive，无管线化、重试、重定向及代理环境变量读取。HTTPS 强制证书校验。认证可从指定环境变量读取完整 Authorization 值，不写入 argv/证据。
- responseEncoding 显式取 identity 或 gzip。gzip 模式发送 Accept-Encoding:gzip，支持服务器返回 gzip 或未压缩响应；identity 模式拒绝 gzip。记录实际编码、线上 body SHA、字节数、解压长度与规范化答案摘要。线上和解压后的 body 均受 maxResponseBytes 保护。

## 分析、质量门与边界

分析器先验证文件大小/SHA、profile/case/load/repeat 对应、phase 完整性、序号和计数守恒，随后检查每条成功请求的答案摘要。缺失或损坏证据直接失败；错误答案、请求失败、丢弃、预算耗尽或驱动高占用在报告中明确列出。

报告提供正确响应 p50/p95/p99/max（nearest-rank），计划到完整响应延迟、调度延迟、错误耗时、解码/校验成本、字节数、CPU/event-loop/RSS 观测；吞吐区分窗口内正确完成数与本窗口准入 cohort 最终正确数。多次运行保留各自分位数与范围，不拼接样本冒充独立重复。

所有报告 `performanceAccepted=false`。无错误的运行是 descriptive-only；出现质量问题是 quality-issues。驱动余量、环境干扰、预热稳定性、A/A 噪声、已知退化检出、饱和阶梯、A/B 可比性和区间仍需后续真实校准。CPU>0.8 核或 event-loop utilization>0.8 仅提示潜在驱动压力，不作为已建立余量的证明。

当前重复顺序固定为 repeat→case→load，每个组合启动独立 runner，warmup/measurement 共享该 runner 的连接池。正式 A/B 必须补齐平衡配对与环境切换，不能用当前固定顺序直接下版本结论。
