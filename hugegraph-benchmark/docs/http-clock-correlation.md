# HTTP 时钟关联组件（独立能力）

本组件保存 Python 宿主单调时钟与同一个 Node 进程的 `performance.now()` 锚点，
离线重建每个事件在宿主单调时钟上的**因果外包区间**。当前提供固定 Node peer 的本地 stdio 采集、
Node 库接口、独立验证和映射。现有 HTTP runner、四臂执行器及已冻结证据均未接入；
已有 phase 相对时间缺少原点锚点，不能据此事后补出精确资源窗口。

```text
Python reference                          Node peer（同一个进程）
    记录 S = monotonic_ns()
    ─────── session / sequence / nonce ────────►
                                       读取进程/boot/time namespace 身份
                                       H0 = hrtime.bigint()
                                       N  = performance.now()
                                       H1 = hrtime.bigint()
                                       再读身份并核对
    ◄────────── 原始 anchor JSON ──────────────
    收完一帧，记录 R = monotonic_ns()

                         N 的宿主时间 ∈ [S, R]
```

整个映射不使用 UTC、`performance.timeOrigin`、相同 origin 假设或拟合 offset。
`H0/H1` 用于单调性和观测速率诊断，不负责缩小 `[S,R]`。
宿主时间是 `time.monotonic_ns()` 的读数尺度；报告同时给出该 API 声明的分辨率。
往返区间不能用于宣称物理时钟具有纳秒精度。

## 接口与示例

在 benchmark 根目录保存 spec，例如 `/tmp/clock-spec.json`：

```json
{
    "schemaVersion": 1,
    "kind": "http-clock-correlation-spec",
    "sourceOrigin": "fixture",
    "sampleCount": 4,
    "intervalMs": 20,
    "exchangeTimeoutMs": 2000,
    "wallMs": 10000,
    "maxEvidenceBytes": 262144,
    "maxRttNs": "2000000000",
    "maxMappingWidthNs": "1000000000",
    "maxDriftPpm": 1000
}
```

这些参数仅演示本地能力，不能作为正式测量参数。`sourceOrigin=linux-procfs` 要求 Linux
`/proc` 身份证据与 `clock_gettime(CLOCK_MONOTONIC)`；其他平台必须明确填写 `fixture`。
两种来源会贯穿所有输出，fixture 不能升级成正式 Linux 运行证据。

```bash
python3 components/http/clock-correlation.py capture \
  --spec /tmp/clock-spec.json --out /tmp/new-clock-capture --node /absolute/path/to/node
node components/http/clock-correlation.mjs verify --evidence /tmp/new-clock-capture
node components/http/clock-correlation.mjs map \
  --evidence /tmp/new-clock-capture --event /tmp/clock-event.json
```

两套入口均有 `--help`。输出为 JSON，成功退出 0，验证失败或映射不可用退出 6。
Python `capture` 的 `ok` 仅表示传输采集完成；必须运行 `verify` 验证原始证据与质量门。
新目录使用 create-only；重复执行相同 `--out` 拒绝且保留旧证据。

`verify` 返回 session ID、Node 身份摘要、来源、boot、分辨率、每个锚点及其宿主区间。
`map` 每次先读全部原始证据并独立验证。两种事件 JSON：

```json
{
    "sessionId": "从verify取得",
    "nodeIdentityDigest": "从verify取得",
    "anchorNonce": "实际已封存锚点的nonce"
}
```

```json
{
    "sessionId": "从verify取得",
    "nodeIdentityDigest": "从verify取得",
    "phaseOriginNonce": "实际phase原点使用的已封存锚点nonce",
    "relativeMs": 12.345
}
```

后一接口要求 `relativeMs` 来自同一进程的
`eventPerformanceNow - originAnchor.performanceMs`，与当前 runner 的减法语义一致。
使用者必须在事件证据中绑定实际 phase 原点；此独立组件无法证明任意外部事件的来源。
未来 HTTP 集成还需把原点 nonce、process/session 身份和事件原始值一起封存。
禁止传入别的进程的值、UTC 差值或猜测出的 phase 原点。

库接口：

```js
import {
    createClockContext, captureAnchor, verifyClockEvidence, mapClockEvent
} from "./components/http/clock-correlation.mjs";

const context = createClockContext({ sourceOrigin: "linux-procfs", sessionId });
const anchor = captureAnchor(context, { sequence, nonce });
// 调用方必须将此函数放在reference往返内；裸anchor没有宿主时间区间。
const verified = await verifyClockEvidence(directory);
const mapping = mapClockEvent(verified, event);
```

`createClockContext` 读取 Node 可执行文件和组件摘要，调用者应在测量准备阶段创建一次。
`captureAnchor` 在每个锚点前后复核进程、boot 与 time namespace 身份，返回完整原始读数。
库级 `mapClockEvent` 只接受同一进程中验证器注册并深冻结的对象。
JSON 反序列化结果、浅拷贝或手写 `verified:true` 都会被拒绝；跨进程使用须重新验证原始证据。
公共 CLI 总是重新验证磁盘证据。

## 映射规则与精度边界

- 实际锚点直接使用该次 `[S,R]`。
- phase-relative 事件先用 binary64 的精确有理数值与相邻可表示数，重建减法舍入区间，
  再向外取整到纳秒；不会直接把大的浮点毫秒值乘一百万并当作精确整数。
- 选取严格早于事件的最近锚点 `i` 和严格晚于事件的最近锚点 `j`，输出 `[S_i,R_j]`。
  相等的舍入读数不能证明先后，因此要求严格包围。
- 没有双侧锚点，返回 `usable=false`、`outside-strict-anchor-coverage`，禁止外推。
- 区间超过预声明 `maxMappingWidthNs`，保留真实区间及宽度，返回 `usable=false`。
  不丢弃慢样本、不取中点、不以更低的 RTT 样本替换失败样本。
- `maxDriftPpm` 仅检查**已观测到的**相邻 host/hrtime/performance 增量区间是否相容。
  门限不改变映射区间，也不证明两锚点之间没有未观测异常。

每秒采一个锚点的区间通常接近秒级。要关联短 measurement phase，应在实际边界附近做往返锚点，
并把锚点开销与测量路径的影响单独校准。当前实现没有完成该集成或校准。

## 原始证据、身份与预算

```text
new-clock-capture/
  spec.json        预声明参数，采样前写入
  session.json     Python时钟实现、boot/进程/namespace、Node/源文件内容身份
  samples.ndjson   每次request/response完整帧、S/R、host前后身份
  result.json      完成/失败、子进程退出码、输入尾部重验、字节计数
  manifest.json    上述四个文件的完整inventory、字节数、SHA-256
```

验证器不信任 result 的成功字符串：必须重建完整样本数量/顺序/nonce、前后身份、时间顺序、
RTT、观测速率及字节账。少一条、失败一条或跨 session/进程均拒绝；重新封存篡改数据不能绕过语义检查。
摘要锁确保内容可复核，并不构成对恶意伪造者的数字签名或远程可信证明。

Linux 身份含 boot UUID、PID、`/proc/self/stat` start ticks、time/time-for-children namespace、
`timens_offsets`，并记录 Python/Node 版本、可执行文件及两组件摘要。stdio 子进程必须继承相同 time namespace。
原始大整数均采用十进制字符串，禁止经过不安全的 JSON number 转换。
非 Linux fixture 使用生命周期随机 token，不能声称具有 Linux boot/PID 证明。

严格 spec 范围：2–1000 样本、间隔 1–60000 ms、单次绝对超时 100–10000 ms、
整体 1000–120000 ms、输出 64 KiB–16 MiB、漂移门限 0–100000 ppm。
请求最大 4096 字节、响应最大 16384 字节，stderr 独立限 2048 字节。
读取时即检查帧上限；无换行长帧与逐字节滴流不会延长绝对截止时间。
固定 Node peer 使用 argv 启动，无 shell；EOF 后正常回收，失败先 TERM，再有限等待与 KILL。
采集开始和结束对 Node/Python 可执行文件及组件重新计算内容摘要；哈希每个块检查共同 deadline。
Node 初始化哈希由 Python 外层 deadline 监督。真正阻塞的内核文件 I/O 和外部 SIGKILL
仍须由外层任务/cgroup 生命周期管理；本地能力组件不冒充系统级服务监督器。
固定 peer 当前没有创建后代进程的路径；未来若接入可派生进程的 transport，需扩展为
leader 退出后仍检查并回收整个进程组，不能直接复用当前的单 peer 回收边界。

真实 Linux stdio 功能验收可预声明 6 个样本、50 ms 间隔、5000 ms 单次超时、30000 ms 整体预算、
1 MiB 输出上限、`maxRttNs="5000000000"`、`maxMappingWidthNs="1000000000"`、
`maxDriftPpm=1000`、`sourceOrigin="linux-procfs"`。外层另限 1 CPU、512 MiB、60 秒。
首个 RTT 包含 Node 启动与身份哈希；可选中间锚点检查 map 命令，但 verify 仍必须接受完整全部样本，
不能删除首个样本挽救 RTT 门。此验收只验证 Linux runtime/协议兼容，不提供 HTTP 性能校准。

## 验证与来源

```bash
node --test tests-normative/http-clock-correlation.test.mjs
python3 -m unittest discover -s tests-normative -p test_http_clock_correlation.py -v
```

Node 测试运行真实本地 Python/Node 往返并离线重验，覆盖时间/身份/nonce/预算篡改、
不安全整数、漏样本、重封存、超宽区间、外推拒绝和浮点舍入。
Linux 形状的单元样本只验证解析器，未在本机宣称 Linux 运行验收。
Python 测试覆盖滴流绝对超时、超长无换行、stderr、重复响应、EOF 和取消回收。
无 HTTP 请求、图数据库实例或远端操作。

依据已核对的主要来源：

- [Node 22.22 performance API](https://nodejs.org/download/release/v22.22.0/docs/api/perf_hooks.html#performancenow)：
  `performance.now()` 是当前进程启动后的高分辨率毫秒读数。
- [Node 22 process.hrtime.bigint](https://nodejs.org/download/release/v22.22.0/docs/api/process.html#processhrtimebigint)：
  纳秒读数的原点任意，不应与日历时间混用。
- [Node 22.22 源码 node_perf.cc](https://github.com/nodejs/node/blob/v22.22.0/src/node_perf.cc)：
  `PerformanceNowImpl` 由 `uv_hrtime()` 减进程起点并换算得到。
- [Node 22.22 所带 libuv Linux 实现](https://github.com/nodejs/node/blob/v22.22.0/deps/uv/src/unix/linux.c)：
  精确 `uv__hrtime` 分支读取 `CLOCK_MONOTONIC`。
- [Python monotonic 文档](https://docs.python.org/3/library/time.html#time.monotonic)：
  使用单调时钟并提供运行时实现/分辨率信息；组件保留实际 `get_clock_info` 结果。

实现选择依赖有界因果顺序。以上来源支持 API 语义和诊断设计，不用于假设不同运行时共享数值原点。
