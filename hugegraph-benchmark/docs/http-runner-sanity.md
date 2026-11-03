# 实际 Node 调用进程的 Python S/R 原始协议

这是 M2 的可选实际 runner 原始采集。HTTP profile 未配置 `clockReference` 时不启动 peer；
配置后，每个 run、preflight 和 postcheck 各创建独立 session。已冻结的 v4/v5 输入保持原样。
`runner-sanity.mjs` 在调用它的 Node 进程中读取 H，`phase-clock-reference.py` 是固定 Python 子 peer。
没有另起 Node peer，没有跨任务 RPC、网络服务或逐请求观测。
正常结果也保持 `verified=false`、`hostAssociation=unverified`、`performanceAccepted=false`。

```text
实际 Node 调用进程                           固定 Python 子 peer
    init(session/run/profile) ────────────────────► 检查参数、实际文件摘要、clock/proc 身份
    ◄─────────────────────────────────────────── ready

    sample(pre) ────────────────────────────────► 读取身份，准备 challenge/nonce
                                             S = time.monotonic_ns()
    ◄─────────────────────────────────────────── challenge 原始帧
    H0 = hrtime.bigint()
    P  = performance.now()       // 就在实际调用 Node 内
    H1 = hrtime.bigint()
    anchor(nonce,H0,P,H1) ───────────────────────► 收全帧，立即读 R = monotonic_ns()
                                             再解析/核对，复核身份
    ◄─────────────────────────────────────────── receipt(S,R,双向原始帧及摘要)

    待包围的工作；peer 阻塞等待，不采样

    sample(post) ──────────────── 同样一次交换 ─────►
    finish ─────────────────────────────────────► 复核身份/实际文件元数据
    ◄─────────────────────────────────────────── finished，退出
    收到真实 exit、stdout EOF、stderr EOF
```

每 session 恰好 pre/post 两次。S 在 challenge 的所有输出字节之前，R 在收到完整回复帧后、
JSON 解析和身份读取之前。记录实际区间，保留传输、调度和校验导致的宽度；不取中点或最佳样本。
Python 通道的绝对截止覆盖逐字节滴流。UUID nonce、顺序、session、run/case/load/repeat/probe、
profile digest 共同绑定交换，禁止重放或跨实例拼接。

## 库接口

```js
import {
    startRunnerSanity,
    captureRunnerSanity,
    finishRunnerSanity,
    stopRunnerSanity,
} from "./components/http/runner-sanity.mjs";

const spec = {
    schemaVersion: 1,
    kind: "http-runner-sanity-spec",
    sourceOrigin: "linux-procfs",
    startupMs: 2000,
    exchangeMs: 1000,
    finishMs: 500,
    wallMs: 10000,
    maxFrameBytes: 8192,
    maxRawBytes: 65536,
    maxStderrBytes: 2048,
};
const binding = {
    sessionId: "实际32位十六进制session",
    runIdentity: { id: "r0001", caseId: "实际题号", loadId: "实际负载号", repeat: 0, probePhase: null },
    profileDigest: "实际64位SHA256",
};
const context = await startRunnerSanity({ python: "/absolute/fixed/python", spec, binding });
await captureRunnerSanity(context, "pre");
// 实际同一 Node 中的工作。此库本身不执行 HTTP。
await captureRunnerSanity(context, "post");
const raw = await finishRunnerSanity(context);
// 调用方独占创建原始证据文件，将 raw 纳入自己的内容摘要锁。
```

context 为 WeakMap 登记的私有对象。调用者不能提供 H/anchor 或指定另一个 Node 进程，
也不能以手写 context 调用接口。`captureAnchor` 复用已测试的 clock-correlation 库，
其 Node 读数、boot/PID/start/time namespace 身份来自实际调用进程。

初始源文件/Node 身份读取在 peer 启动之前进行，均应置于未来实际测量窗外。
Node 源文件最多 1 MiB，既有 Node 时钟库的可执行文件摘要上限为 1 GiB；
这部分与单次内核读取的硬截止由外层 task/cgroup 负责。
`startupMs/wallMs` 从 peer 派发开始计时，不是整个调用方准备阶段的硬运行预算。

Python 使用固定 argv `-I -S -B` 和相邻固定 peer 文件，清空继承环境后只保留 locale。
Authorization、用户 Python 路径和启动配置不会传入 peer。
`python` 参数必须是绝对路径；路径选择与来源锁分别核对，提供路径不等于证明其可信来源。

## HTTP profile 接线

profile 可选字段使用同一严格 spec；下列数值只用于展示接口，实际时间和质量预算需随实验声明。

```json
{
    "clockReference": {
        "python": "/absolute/fixed/python3",
        "spec": {
            "schemaVersion": 1,
            "kind": "http-runner-sanity-spec",
            "sourceOrigin": "linux-procfs",
            "startupMs": 2000,
            "exchangeMs": 1000,
            "finishMs": 500,
            "wallMs": 30000,
            "maxFrameBytes": 8192,
            "maxRawBytes": 65536,
            "maxStderrBytes": 2048
        }
    }
}
```

配置缺省表示不启用。`python` 必须指向此次执行环境的绝对路径；若由 agent 使用
`HGBENCH_PYTHON` 选择路径，应将选择结果写入 profile。runner 使用已冻结的字段，不依赖环境变量
临时切换 peer。fixture 必须显式声明，不能替代 Linux 生产身份。

prepare 在实际 phase 前完成源文件/身份准备及 pre 交换；finish 在所有 phase（含 drain）后完成
post、身份/元数据复核和实际 exit/双 EOF 收尾。各 phase 边界继续只有三次时钟读取，peer 在中间阻塞
等待。`run.clockReference` 保存完整 raw transcript，`phaseClock.clockReference` 保存严格 marker：
`required:true`、profile/spec 的规范内容摘要和 raw 载荷预算。

`validateDirectPhaseClockRaw(run, profile)` 用真实 profile 期待值拒绝 marker/raw 双删、意外添加或
摘要不符，再核同 session/run/PID、Node 版本/实际 executable SHA、已知 boot/namespace 与前后包围。
两模块各自的 processToken 分别核其内部一致性，跨模块用实际进程身份关联。collector 身份及外部
运行时来源仍由独立核验处理；它们没有因本次 raw 自洽而关闭。

## 独立核验

```bash
node components/http/runner-sanity.mjs --help
node components/http/runner-sanity.mjs verify --evidence /absolute/raw.json
python3 components/http/phase-clock-reference.py --help
```

`verify` 是只读入口，不启动 peer。它重新读取有界原始文件，核对十二个双向帧的真实字节、
长度、SHA、完整写入标记、顺序、nonce、绑定与 receipt 中保留的原始 challenge/response。
重复 JSON key、非有限数值、非规范纳秒整数及超深 JSON 被拒绝。
核验同时检查实际 Node PID、一致的前后身份、Python parent PID 与调用者关系、
Python 文件摘要格式/前后元数据，以及正常退出和双 EOF。单独的成功回执无法替代原始帧。
Node 的 runtime/processToken 与 Python 的 implementation/clock 等身份字段必须完整且符合实际 producer
结构；删字段后重封原始帧仍会被拒绝。Node anchor 的组件 SHA 同时绑定本次 `clock-correlation.mjs`
源文件记录。这是原始证据内的一致性检查，外部源码和运行时期待值仍须独立提供。
Python ready 中的 peer 脚本 path、元数据和 SHA 必须与 Node 独立读取的该脚本完全相同，
前后文件元数据也必须一致。Linux Python executable 记录必须来自实际 `/proc/self/exe`；
离线验证不会通过当前机器的 symlink 解析替代其他平台当时的 executable 来源。

Linux 模式额外检查 Node/Python 的 boot、time/time_for_children namespace 和原始 offsets 一致，
并记录本次 `S ≤ H0 ≤ H1 ≤ R` 的数值相容性。fixture 保持显式 fixture，宿主字段为空，
数值相容性返回 null。未知来源不会转成生产 Linux 证据。

成功仅返回 `rawSanity=passed`，始终保留三项 pending：

1. `external-runtime-source-lock`：来自外部冻结计划的 Node/Python/镜像/加载环境和组件源码期待值。
   当前文件摘要是原始自观测，尚未与这样的外部期待值比较。
2. `host-collector-namespace-binding`：此次实际 collector 的 clockIdentity、runtime 与同 boot/time namespace。
   这个 peer 与 Node 的一致性不能替代宿主 collector 绑定。
3. `predeclared-quality-thresholds`：正式声明的 RTT、边界宽度及适用窗口门限。
   当前传输超时只保护执行，不是正式测量质量阈值。

两个交换的因果先后已经在数据中保留。将 Node 的 H 直接解释为宿主时间，还需固定实现与上述来源门。
通用 API 的任意原点说明、相同版本、自报 SHA、一次数值相容性或历史独立 peer 数据均不能独自证明它。
此原型没有开放 `verified=true` 或凭外部布尔值放行的接口。

## 预算、失败与清理

输入严格拒绝未知字段，预算范围通过 `validateRunnerSanitySpec` 与 Python 独立检查。
示例参数仅用于功能原型，正式阈值待 M2 校准。

| 预算               | 范围与含义                                                  |
| ------------------ | ----------------------------------------------------------- |
| startup / exchange | 各 100–10000 ms，绝对截止覆盖完整帧                         |
| finish             | 50–2000 ms，TERM/KILL/EOF 回收宽限                          |
| wall               | 1000–600000 ms，包含 peer 常驻等待；独立 watchdog           |
| frame              | 4096–16384 B；示例 8192 B 适合完整身份回复                  |
| raw                | 16–256 KiB，所有双向传输及 stderr；至少四倍 frame           |
| stderr             | 0–4096 B，超限截断保留并明确失败                            |
| Python 摘要        | 单文件 256 MiB，在 startup 截止内逐块检查；结束只复核元数据 |

没有自动重试、换 peer、重放、后台采样或恢复同一 session。
启动/交换/结束失败抛出带 `code` 和 `evidence` 的错误；后者是 partial transcript、
保留的 stdout 片段/stderr、真实字节计数和终态，调用者须将它写入失败证据。
`stopRunnerSanity` 供调用方取消和 finally 清理；未完成时返回 partial，已结束时幂等返回原回执。
`startRunnerSanity` 可接受非序列化的 `signal:AbortSignal`，覆盖启动、交换和闲置等待。
未关闭的 peer 先 TERM、再 KILL，重复取消不重复发送 TERM 或延长宽限，
最终保存实际退出码/信号、是否强杀、双 EOF。peer 继承任务进程组，不 detach，也不产生子进程。
wall watchdog 在调用者暂时没有继续读取时仍会触发清理。
正常完成与失败清理均移除 abort 监听。runner 的 catch/finally 等待清理完成；失败会独占创建
`${tracePath}.clock-reference.partial.json`，保留有界原始回执或明确 null（尚未启动 peer）和失败代码，
不写成功 run JSON。副本写入或 fsync 出错同样失败，内存异常保留原回执供上层诊断。
外层 task/container 继续拥有不可中断 I/O 和最终进程组保护；清理截止或缺少 EOF 不会变成 completed。

`maxRawBytes` 约束双向 wire/stderr 原始字节，不能当作完整 JSON 上限。raw transcript 的 compact JSON
硬上限为 `4 × maxRawBytes + 65536`；完整 pretty run/probe 和 partial sidecar 上限为
`4 × maxRawBytes + 131072`。原 phase clock 的 64 KiB 子预算继续独立执行。profile/比较计划为所有
measurement 和前后探针计入这些上限，超出 32 MiB 元数据总门时拒绝计划；不会扩大总门。
外层 workflow deadline 同时为每个 peer 的 wall/收尾预留时间。未知来源、缺失阈值或 namespace 绑定
仍为 pending，不以传输超时替代正式质量门限。

## 验收与下一步

主接线保持单一用途，无需修改通用 task/guardian 协议：

- profile、runner、workflow/probe、analysis 的可选 raw 接线及明确预算已经实现。
- runner-sanity、固定 Python peer、clock-correlation 和实际 phase 组件一起纳入实现内容锁。
- 后续由 paired bridge 关联当次 collector clockIdentity 与外部冻结来源，逐项关闭来源和质量门。

本地测试使用实际调用 Node、真实 Python 子进程和本地 HTTP 工作，覆盖原始篡改、错序、取消、
EOF、滴流、stderr 超限、独立 wall 清理和只读 CLI 重建。Python 故障测试使用显式 synthetic fixture，
覆盖错误 nonce/顺序、等待截止及临时源码变化；其人工回复从不声称来自实际 Node。
集成专测另覆盖真实 runner SIGTERM、启动中的 TERM-ignoring peer、listener/进程回收、profile 期待值、
完整重封后的 phase 包围/Node 来源冲突、探针和可重建的本地 workflow。

Linux 真实验证尚未执行。下一次新证据运行应冻结这两个新组件及运行时，
在已授权 Docker driver 内使用实际固定 Node/Python、原地新建输出目录，执行同样两次真实调用进程交换，
核对 PID/start/boot/namespace、原始 S/R/H、预算与实际清理，同时启动新的 collector 保存当次身份。
然后在本地以冻结来源重建 raw verdict并逐字节比较；来源、collector 绑定和质量门未完成时仍保持 unverified。
此组件功能验证不产生图数据库性能结论，也不替代 HTTP 正式校准或 SF100 验收。
