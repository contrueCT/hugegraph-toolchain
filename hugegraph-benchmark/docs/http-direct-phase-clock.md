# HTTP 实际 phase 边界原始证据

HTTP runner 为 warmup、measurement、preflight、postcheck 保存实际起止边界。
每个边界读取 `hrtime.bigint() → performance.now() → hrtime.bigint()`，形成原始区间。
这些读数用于后续将 HTTP phase 与资源、GC/JIT 观测关联；当前输出保持
`verified=false`、`hostAssociation=unverified` 和 `performanceAccepted=false`。

```text
测量前                       实际 phase（含 drain）                        测量后
  身份、文件摘要       H0 → performance.now() → H1  ...  H0 → performance.now() → H1      身份、文件复核
  preparedInterval              start                         end             finishedInterval
       └──────────────────────────── 同一个 session / run / PID ───────────────────────┘
```

## 运行与接线

`runner.mjs` 自动采集，现有 HTTP CLI、workflow、probe 入口沿用原参数。
新增数据位于每份 run/probe JSON 的 `phaseClock`，以及 `phases[].clock`。
原始请求 trace 保留 phase-relative 毫秒字段，无逐请求时钟载荷、文件摘要或进程读取。

组件提供下列库接口，无独立启动命令：

```js
import {
    prepareDirectPhaseClock,
    captureDirectPhaseBoundary,
    recordDirectPhaseClock,
    finishDirectPhaseClock,
    validateDirectPhaseClockRaw,
} from "./direct-phase-clock.mjs";

const context = await prepareDirectPhaseClock({ id, caseId, loadId, repeat, probePhase });
const start = captureDirectPhaseBoundary(context);
// 原有 HTTP phase 工作，包括实际 drain。
const end = captureDirectPhaseBoundary(context);
const elapsedMs = end.performanceMs - start.performanceMs;
const clock = recordDirectPhaseClock(context, phaseName, start, end, elapsedMs);
const phaseClock = await finishDirectPhaseClock(context, [{ clock }]);
// 将 clock 放入对应 phase，将 phaseClock 放入所属 run/probe。
const consistency = validateDirectPhaseClockRaw(run);
```

context 使用进程内私有 WeakMap 注册，完成后不可再次读取边界或结束。
传入手写对象、复制的 context 或已经结束的 context，返回 `DIRECT_CLOCK_CONTEXT`。
`prepare` 和 `finish` 应位于测量窗外；边界函数只执行三次时钟读取和最小对象创建。
phase 的 `elapsedMs` 直接采用保存的两个 `performanceMs` 之差，保持原有计时口径。
实际 end 包含延迟响应及 drain，其时间可能超过声明的 `durationMs`。

可选 profile `clockReference:{python,spec}` 将真实 Python pre/post S/R 交换接入 prepare/finish，
完整配置及协议见 [实际 runner S/R](http-runner-sanity.md)。缺省不启动 peer；启用后仍保持 raw/unverified。
run 顶层 `clockReference` 保存原始回执，session 内同名 marker 绑定 required、profile/spec 摘要与预算。
测量中的边界函数和逐请求 trace 不增加 IPC、文件读取或摘要工作。

```text
prepare：身份/摘要 → S/R pre → prepared.end
                                 │
                  phase start → 实际工作及drain → phase end
                                                           │
                      finish.start → S/R post → exit/双EOF → 身份复核
```

独立验证应调用 `validateDirectPhaseClockRaw(run, profile)`，按实际 profile 检查 marker/raw 是否必需，
防止双删后伪装成未启用格式。省略第二参数只核已有证据的内部一致性。
库调用者可在 prepare 的第二参数传入 `{signal}`；启用时的输入还需传入 `clockReference` 与
`profileDigest:valueDigest(profile)`。`getDirectPhaseClockReferenceRaw(handle)` 在结束后返回该 session
原始回执；`stopDirectPhaseClock(handle,reason)` 用于 finally 幂等收束，runner 已调用并等待。
失败时保留独占创建的 `${tracePath}.clock-reference.partial.json`，不写成功 run 结果。

## 身份、来源与失败

session 保存下列原始信息：

- session ID、run/case/load/repeat 绑定，功能探针额外绑定 preflight/postcheck。
- Node/uv 版本、平台、架构、共享 libuv 配置、启动参数摘要及注入环境变量是否存在。
  不保存环境变量值、请求认证信息或完整启动参数。
- Node 可执行文件与本组件的 SHA-256、字节数、设备、inode、mtime/ctime 纳秒值。
  Linux 通过 `/proc/self/exe` 读取实际运行的可执行对象，同时保留 `process.execPath`。
- Linux boot ID、PID、进程启动 ticks、原始 `/proc/self/stat` 和 status、time/
  time_for_children/pid/mnt/user namespace 链接及 `timens_offsets`。
- 测量前准备与测量后复核的 hrtime 区间，便于检查所有 phase 窗确实在两者之间。

摘要首次在进程的准备阶段读取，后续准备与结束复核文件元数据。首次读取前后也核对元数据，
超出预算、身份变化或已缓存文件的元数据变化均拒绝。ctime 变化包括硬链接增减，
因此测试夹具应复制实际 Node 二进制，避免对正在使用的可执行 inode 建立临时硬链接。
文件摘要读取限制为每文件 256 MiB，不在 phase 边界或请求路径重新扫描文件。

读取不可用时保留具体原因，session 标为 `unavailable`。未知 Node 版本和不同平台仍可保留
原始本地计时；它们没有已建立的宿主关联契约。观察到身份、namespace、启动状态或文件漂移，
整次 run 失败，已写入的有界原始 trace 保留用于诊断，不生成成功 run 结果。

`validateDirectPhaseClockRaw` 独立核对 run/phase 绑定、身份前后一致、十进制纳秒字符串、
起止顺序、实际 elapsed、所有 phase 位于准备与复核之间，以及证据字节预算。
`rawValidated=true` 表示这组原始字段自洽，不表示其已经关联到宿主时钟。
旧 run 同时缺少 session 和所有 phase clock 时，返回 `legacy-no-phase-clock`；
仅删除 session 或单个 phase clock 会被拒绝，防止不完整的新证据被当作旧格式。

## 待完成的宿主关联门

固定 Linux x64 官方 Node v22.14.0 / libuv 1.49.2 的已核实可执行文件，且无已记录的注入环境、
身份信息完整时，session 可列出 `contractCandidate=linux-node22.14-cpython-monotonic-v1`。
candidate 仅用于选择待验证规则。下列六项仍列在 `pendingGates`，全部满足前不得升级 verified：

1. `runtime-provenance-lock`：固定实际 Node/CPython 二进制、版本与对应时钟实现来源。
2. `host-python-clock`：实际 Python 的 monotonic_ns 实现、时钟声明和宿主身份。
3. `host-namespace-binding`：同 boot、实际 PID 启动身份、宿主/驱动 time namespace 与 offsets 绑定。
4. `raw-roundtrip-sanity`：在真实同一 Node 进程上保留 Python S/R 与 Node H0/H1 的因果包围原始读数。
5. `predeclared-boundary-width`：先声明允许的边界区间宽度，保留超限读数及不适用原因。
6. `component-source-lock`：将实际 runner、时钟组件、运行参数及实现文件摘要绑定到此次不可变证据。

版本或摘要不匹配、namespace 缺失、来源未知以及任一质量门缺失，保持 unverified/unavailable。
当前原始验证器不提供手写 `verified=true` 的入口；此字段和 pendingGates 的篡改会被拒绝。

固定实现的源码链与官方归档成员摘要见
直接时钟来源研究（本机记录未随包分发）。
该研究的独立 peer 样本不能证明后续 runner 的进程身份和运行时状态。

实际驱动进程已可在测量窗外保存有界 S/R sanity，六门仍保持 pending，等待当次外部来源、collector
绑定与预先声明的质量阈值独立核验。后续分析应保留边界区间，禁止取中点充当精确时刻，
也不能用历史独立 peer 的时钟证据替代此次实际 runner。

## 预算与验证范围

`maxEvidenceBytes=65536` 只约束序列化 `{session, phases: phase clocks}` 的时钟载荷，
不包含完整 run/probe、HTTP trace、JSON 缩进或其他诊断数据。probe 与工作目录各自的外层预算继续生效。
CLI/profile planner 在原有 32 MiB 元数据门内为每个 measurement run 和前后探针预留该上限；
comparison planner 也将这些 session 纳入每臂证据余量，超出时要求拆分 profile。
启用 S/R 后，顶层 raw transcript 另有 `4 × maxRawBytes + 65536` compact 载荷上限；完整 pretty
run/probe 和失败 sidecar 上限为 `4 × maxRawBytes + 131072`。它们计入原 32 MiB 总门，
probe 的 JSON 读取上限按此明确预算调整，HTTP trace 的独立上限保持原口径。

专用规范测试：

```bash
node --test tests-normative/http-direct-phase-clock.test.mjs
```

它使用真实本地 HTTP 请求覆盖 warmup/measurement、延迟响应实际终点、功能探针、
启动状态变化失败、临时组件副本实际变动失败，以及 session/phase/elapsed/精度/预算篡改。
这些测试验证原始功能与失败边界，不证明正式性能窗口的观测开销、宿主关联精度或统计有效性。
