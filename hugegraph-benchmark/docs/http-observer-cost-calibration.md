# HTTP 观测、校验与 trace 成本校准

这是一份待执行的有限操作方案。当前 004 A/A 保持原样；本方案不派发远端任务，不增加时钟证明，
不改变正式运行器。先完成 A/A、已知退化及驱动余量/预热校准，再在同一已声明负载上执行下面两组实验。

本地有限生成器为 `.hgbench/http-observer-cost-plan.py`，复用固定 known-delay 规划 helper。
`draft` 真实调用冻结 CLI 的 16 组 generate/validate/HTTP plan、12 份 collector task plan；
collector 另 4 臂明确关闭。`prepare` 仅允许 Linux 非 root owner，严格核对当前声明的重任务单位和实际 build，
再生成 16 份全新 environment plan。两个命令均不派发、不启动 Docker、不发送 HTTP。
默认只读复用 004 的 456 文件源快照；本操作的配置和结果不改变 004 A/A。

```bash
python3 -B .hgbench/http-observer-cost-plan.py draft --out .hgbench/http-observer-cost-draft-v2
python3 -B .hgbench/http-observer-cost-plan.py verify --out .hgbench/http-observer-cost-draft-v2
# 未来 Linux owner 上准备新计划，尚未执行；每个当前校准任务都要列入 inactive 检查。
python3 -B .hgbench/http-observer-cost-plan.py prepare --out /absolute/new-observer-cost-attempt \
  --source-root /absolute/frozen/source --source-lock /absolute/frozen/source.lock.json \
  --require-inactive-unit CURRENT_AA.service --require-inactive-unit CURRENT_KNOWN_DELAY.service \
  --require-inactive-unit CURRENT_HEADROOM.service
```

`plan.lock.json` 锁定完整产物清单、生成器与固定 helper；`source-selection.lock.json` 和
`source-verification.json` 记录实际选定源。`budgets.json` 逐步求和并核对容器、collector 和全操作寿命。
离线 collector plan 指向显式无效的 `needs-live-binding` spec，无法直接采集；它只验证公共 task 契约。
实际执行前必须据同一 attempt 的真实 CID/PID 新写 `collector-spec.json`，把它纳入 trackedFiles，
重新通过公共 `task plan` 取得 bound plan。不得提交离线 unbound plan。
生成动作的输出保持 `dispatchImplemented=false / executed=false / performanceAccepted=false`；
另由下面独立的固定执行器解释这套 recipe，不修改既有计划。

`.hgbench/http-observer-cost-execute.py` 已提供 `inspect / freeze / run / status / stop`。
它锁定 known-delay 的 guard/command/容器归属实现和本计划源，核对全部静态输入、逐步预算及两组原 policy。
freeze 会创建独立 execution spec；draft 只能得到 `runnable=false`。当前交付经过本地协议/故障测试和真实公共
task planner 验证，Linux Docker 真实执行尚未进行。未来运行入口如下，路径及 SHA 都应取自该次新 prepare：

```bash
python3 -B .hgbench/http-observer-cost-execute.py inspect \
  --plan /absolute/prepared-cost-plan --expected-plan-sha256 ORIGINAL_PLAN_LOCK_SHA256
python3 -B .hgbench/http-observer-cost-execute.py freeze \
  --plan /absolute/prepared-cost-plan --expected-plan-sha256 ORIGINAL_PLAN_LOCK_SHA256 \
  --out /absolute/new-cost-execution
# 仅未来 Linux owner；外层须用独立 user service 约束 1CPU/512MiB/128PID，
# RuntimeMaxSec=55100、TimeoutStopSec=100。此处没有实际派发。
python3 -B .hgbench/http-observer-cost-execute.py run --execution /absolute/new-cost-execution
python3 -B .hgbench/http-observer-cost-execute.py status --execution /absolute/new-cost-execution
python3 -B .hgbench/http-observer-cost-execute.py stop --execution /absolute/new-cost-execution
```

原 prepare plan 只可 claim 一次；其他 execution 目录也不能重放它。每次提交前持久化原 request/task ID、
实际 public plan digest 和 dispatch-once 标记。正常路径以受控 collector stop、原 task 终态、独立资源核验完成后，
才移除 anchors。故障收尾共用 90 秒；查询/取消丢失时继续尝试确切归属清理，保留 partial/unknown，停止后续臂。
stop 只发布持久化请求，status 只展示已保存状态，二者都不证明清理完成。

JVM-on 在完整 capture 后运行主 parser 与独立重算，两次共享原 `parseJvm=10000ms` 的绝对截止。
这项上限不因第二次解析而重置；剩余时间不足会保留该臂 invalid。JVM-off 保留应用 stdout，明确没有 JVM 观测回执。
HTTP raw、正常生命周期、实际 settings、collector/JVM 分支全部通过后，执行器才构造两组原 policy 的统计输入。
坏臂/缺臂保留在全部 16 臂和各组四对分母中；不足 1000 个正确 measurement 观测保持 unknown。
真实运行、开销资格、预热稳态及精细时钟归因都尚未验收，性能/资格标记始终 false。
每臂完成 raw/lifecycle/JVM 复核后再次检查全部声明单位及受保护 d6，最后一臂也执行此检查。
pre/post 每轮的全部单位共用原 `cleanWindow=10s` 绝对截止；post 另消耗原 arm/operation 剩余时间，
不会扩展旧计划的总硬截止。状态变化或时间不足均使该臂 invalid。

```text
同构建、同 forward、同 11 题、同驱动、正确性和 trace 始终开启
                 |
       +---------+----------+
       |                    |
  J：JVM 观测开关       C：独立 collector 开关
  collector 两臂开启    JVM 观测两臂开启
       |                    |
  新 JVM 的 J0/J1       新 JVM 的 C0/C1
       +---------+----------+
                 |
      各自原始核验、配对效应、unknown
      两个条件效应不相加为“总成本”
```

## 现有能力与最小接线缺口

| 项目      | 现在能做的操作                                                                                                                                                                           | 本方案的边界                                                                              |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| JVM 观测  | 新的 `http environment plan` 可省略或提供 `jvmObservation`；启用时 server 增加固定 `-Xlog:gc*=info,safepoint=info,jit+compilation=debug:stdout:timenanos,uptimenanos,pid,tid,level,tags` | 比较启用整项的运行扰动，含同步日志与现有 capture 路径；不能独立称为某一条日志指令的成本   |
| collector | 独立 `resource-collector.py collect` 可在 HTTP task 前启动、受控停止；off 臂不启动它                                                                                                     | 现有 `comparison-workflow.py` 强制启动 collector，没有 off 字段；拉长 interval 不等于关闭 |
| validator | 每个完整响应已有 `validationMs`，gzip 响应另有 `decodeMs`                                                                                                                                | 正式校验保持开启；没有受支持的 validator-off 运行模式                                     |
| trace     | 每个 run 已有 `evidenceWriteMs`、`traceBytes` 和原始记录数                                                                                                                               | 没有 trace-off 运行模式；不得删除 trace 后宣称可核验的正式吞吐                            |

公共 comparison planner 固定共同 settings、只比较 mode，不能直接表达本方案的两个单变量。
`.hgbench/http-known-delay-execute.py` 严格核对固定 sleep10 请求、无 JVM 观测的 settings 和 16 臂序列，
不能把它的 JSON 改成成本实验再运行。成本执行器复用其有界命令、目录保护和目标归属检查，
单独固定这两组序列及 collector 开关；以下 argv 由该有限执行器组织，无需修改正式运行器。

## 冻结输入、顺序和统计族

- 使用选定、完整锁定的源快照及同一 HugeGraph distribution、JDK、Node、镜像；不引用“当前主树”作为运行时。
  环境观察在执行时重新采集。三项旧重任务允许严格的 `loaded-inactive` / `unit-absent`；d6 继续运行并披露。
  A/A 或已知退化操作仍活动时不派发本操作；该检查不证明主机独占。
- 使用完整 HTTP serving 11 题，`purpose=calibration`、`serviceChecks=true`、gzip、forward、无可选 S/R peer。
  两臂相同 `open=10 requests/s`、`maxInFlight=4`、`maxStartDelayMs=1000`、预热 30000ms、测量 120000ms、
  request/drain 各 5000ms，`maxRequestsPerPhase=2000`、响应上限 2MiB、全 workflow trace 上限 256MiB。
  每题计划预热 300、测量 1200 次；10rps 是此配置的待校准负载，不是饱和率声明。
- J 组：A=`JVM off / collector on`，B=`JVM on / collector on`。
  C 组：A=`JVM on / collector off`，B=`JVM on / collector on`。
  每组固定 4 个独立 pair、两个 AB/两个 BA；用现有 `components/paired/core.mjs` 的 `balancedOrders`
  和分别预声明的 order/resample seed 生成顺序。共 16 个全新 JVM，B 臂也不能跨组复用。
  两组的先后顺序一并在 dispatch 前冻结，不依据先看到的效应调整。
- 每组预声明 22 个主指标：每题 `correctEndToEndMs.p99` 和 `correctResponseMs.p50`。
  每臂、每指标至少 1000 个正确 measurement 观测；不合并 warmup、pair 或实验补足。
  全部两组的 family alpha 总额 0.05，各分 0.025；每组 bootstrap=80000、minimumTailDraws=20、minimumPairs=4，
  work=4×80000×22=7040000，尾期望 80000×0.025/(4×22)=22.727。
  现有 paired analyzer 的上限取 `maxWork=1000000000 / maxMilliseconds=60000`。
  延迟实际影响预算预声明为相对 5% 与绝对 1ms，沿用既有同时超过两项的关系判断。

四对只给首组条件效应诊断，不能证明 bootstrap 覆盖率或普遍低开销。缺样本、缺臂、质量失败、零基线、
退化分布及跨预算区间都原样保留 unknown/invalid/inconclusive；所有工具的性能/推断验收标记继续为 false。
goodput、CPU、ELU、event-loop delay、日志/采样字节为诊断项。固定低速下 goodput 相同不能证明采集没有影响。

## 原命令与每臂操作

`SOURCE/NODE/DOCKER/ARM/OWNER/DRIVER_CID/TARGET_CID` 都必须来自该新计划及真实回执；DOCKER 为锁定的绝对路径。
以下是 Linux 操作模板，
不包含示例 CID/PID，不允许跨 attempt 手填或从其他执行借用。每个子命令留存 argv、退出码、stdout/stderr。

1. 保存共同 build/runtime/host/workload，以及该臂 settings、开关表、pair/order、资源和统计 policy。
   J0 的 settings 省略 `jvmObservation`；J1/C0/C1 使用同一原有 spec。其他设置保持一致，mode 均为 forward。
   两臂应用日志捕获、日志字节上限、内存、CPU、线程、磁盘保护相同。为本操作冻结全新环境和任务：

    ```bash
    "$NODE" "$SOURCE/dist/cli.js" http environment plan "$ARM/settings.json" --out "$ARM/target"
    "$NODE" "$SOURCE/dist/cli.js" --root "$ARM/target-tasks" task run \
      --plan "$ARM/target/task.json" --detach --request-id "$TARGET_REQUEST_ID"
    "$NODE" "$SOURCE/dist/cli.js" http environment status "$ARM/target"
    ```

    只轮询该 request-id/环境，180s 内 ready 后绑定 target CID；核对 attempt/plan labels、镜像和 Docker inspect。
    在测量外按现有 mode probe argv 做 pre/post 检查，使用该 ready 中的 Java PID 和 jcmd 路径。

2. 按已知退化操作已有 create argv 创建该臂专属 driver：同一镜像、非 root、1 CPU/768MiB/128 PID、
   read-only、cap-drop ALL、no-new-privileges、无 Docker 日志、64MiB tmpfs、私有 target 网络，
   只读挂载冻结 SOURCE/Node，唯一可写 `/evidence`。PID1 为固定 Python sleep2520，不设自动重启。
   保留 create/start/inspect、cidfile、原 request label 和全部 mount/资源事实。不能使用宿主 loopback 跑 HTTP。
   `/evidence/suite-settings.json` 中附件路径必须是对应容器内路径；然后在真实 driver 内计划：

    ```bash
    "$DOCKER" exec --user "$OWNER" "$DRIVER_CID" /node/bin/node /source/dist/cli.js \
      http generate /evidence/suite-settings.json --out /evidence/profile.json
    "$DOCKER" exec --user "$OWNER" "$DRIVER_CID" /node/bin/node /source/dist/cli.js \
      http plan /evidence/profile.json --out /evidence/http-plan
    ```

3. collector-on：从同一 CID 的 inspect 绑定 target 和 driver 的**宿主 State.Pid**，作为相应 cgroup 的 anchor。
   此处采集目标容器整体，包括 wrapper/JVM，不声称仅 JVM CPU。冻结 collection spec：
   `intervalMs=1000 / durationMs=2300000 / maxSamples=2301 / maxOutputBytes=33554432`，
   `filesystemPath` 为该操作目录，roles 为 target/driver，并使用原有 `controlledStop:{path,token}`。
   用通用 task 冻结并启动下面的固定命令，`effect=filesystem`，timeout=2310000ms、TERM grace=5000ms、日志 1MiB：

    ```bash
    /usr/bin/python3.12 -B "$SOURCE/components/http/resource-collector.py" collect \
      --spec "$ARM/collector-spec.json" --out "$ARM/collector" --json
    ```

    collector task source 的 command 即上述 argv，cwd 为该 ARM，trackedFiles 为固定组件与本臂 spec；
    仍经公共入口取得真实 task 身份：

    ```bash
    "$NODE" "$SOURCE/dist/cli.js" task plan "$ARM/collector-task.source.json" --out "$ARM/collector-task.json"
    "$NODE" "$SOURCE/dist/cli.js" --root "$ARM/collector-tasks" task run \
      --plan "$ARM/collector-task.json" --detach --request-id "$COLLECTOR_REQUEST_ID"
    "$NODE" "$SOURCE/dist/cli.js" --root "$ARM/collector-tasks" task status --request-id "$COLLECTOR_REQUEST_ID"
    ```

    必须观察到首条完整 sample 才释放 HTTP task，最多 30s。collector-off 不启动 collector，
    保留明确的 off 计划与分支回执；不造空 collector 结果。两臂都不在测量期间启动其他资源观察器。

4. 启动真实 HTTP task；调用前持久化原 request/预注册 task ID、真实 plan digest 和 dispatch-once 标记。
   正常路径只启动一次前台 CLI，CLI 内部等待；测量中不反复启动 Docker exec/Node status，不重发：

    ```bash
    "$DOCKER" exec --user "$OWNER" "$DRIVER_CID" /node/bin/node /source/dist/cli.js \
      --root /evidence/tasks task run --plan /evidence/http-plan/task.json --request-id "$HTTP_REQUEST_ID"
    # 仅调用失败/连接丢失后的原身份查询，随后收尾；不能再次 run。
    "$DOCKER" exec --user "$OWNER" "$DRIVER_CID" /node/bin/node /source/dist/cli.js \
      --root /evidence/tasks task status --request-id "$HTTP_REQUEST_ID"
    ```

    首次派发前记录宿主单调时刻，观察到 HTTP task terminal 后记录宿主单调时刻。
    collector-on 按[既有原子停止协议](http-resource-collector.md#执行器受控停止协议)发布 stop marker，
    保留实际 collector spec 原字节摘要/token/boot 和停止文件。这里是包含整个 HTTP task 的粗窗口，
    marker 中的结束时刻为观察到终态后的宿主时刻，不冒充某个客户端 phase 结束纳秒。
    collector 完成及最后覆盖 sample 必须在撤销 anchor 之前取得；SIGTERM/cancel 不能冒充正常完成。

5. 检查真实 HTTP task artifacts、post mode、driver/target 的无 OOM/正常退出/原身份移除回执。
   正常停止原 target 用 `http environment stop "$ARM/target"`，异常时只取消原 HTTP/target/collector task，
   只清理本 attempt 核验过的 CID。任何失败停止后续臂，保留既有输出和预声明缺项。
   JVM-on 使用原 JVM parser（10s、8MiB raw、32MiB parser output、4096 windows、100000 events），
   取得完整 capture 后离线解析；不把解析与下一臂负载同时运行。JVM-off 明确无该组日志/回执。

6. 每臂重新打开完整 raw，由公共命令独立复算：

    ```bash
    "$NODE" "$SOURCE/dist/cli.js" http report "$ARM/driver-evidence/http-plan/output" --out "$ARM/rebuilt-http"
    "$NODE" "$SOURCE/components/paired/analyzer.mjs" \
      --input "$GROUP/verified-observations.json" --output "$GROUP/paired-analysis"
    ```

    第二条命令前由有限操作层核对真实顺序、新 JVM、settings 唯一开关、全部正确答案及 HTTP 计数、
    raw.lock SHA、collector 分支与目标回执，再构造既有 normalized-paired-observations。
    主指标映射是第 2 节所列字段；observations 取该指标的实际正确 measurement count。
    generic paired analyzer 不自行证明这些外部事实，固定执行器仅在前述原始核验通过后映射输入，
    不能直接手填 `status=ok` 取得资格。

## 有限资源预算

| 层级                       | 本方案上限                                                                                                                                |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 目标 / driver / 操作控制器 | 2 CPU / 2GiB；1 CPU / 768MiB；1 CPU / 512MiB，控制器 PID 上限 128；单臂顺序执行                                                           |
| HTTP                       | 完整 11 题 base deadline=2013000ms；task timeout=2073000ms；176 个 measurement run，352 个 pre/post case probe                            |
| 容器 TTL                   | target=3000s，driver PID1=2520s；逐步上限各需2762s/2323s，含 ready、probe、驱动准备、HTTP wait2108s、collector 停止及清理余量             |
| collector-on               | 1s 间隔、2300s 最大期、2301 samples、32MiB；从启动至首样/HTTP/收尾需2198s；预算耗尽仍保留 partial                                         |
| 每臂收尾                   | 90s 共享清理；单命令最长 45s、输出 2MiB；mode 前/后各 45s+2s；独立 HTTP report 300s；JVM parse 10s                                        |
| 总工作                     | 全部 step 相加每臂3182000ms；16臂加统计120000ms/封存60000ms/共享清理90000ms=51182000ms；上限55000000ms，停止宽限100s                      |
| 证据 / 磁盘                | 每臂target512MiB/raw512MiB（含trace256MiB、collector32MiB）/analysis96MiB/命令64MiB，加封存256MiB共18.75GiB；work cap32GiB，持续80GiB空闲 |
| 离线分析                   | 每组最多60s、输入8MiB、输出64MiB；所有 pair/指标固定，不能删坏臂缩小统计族                                                                |

以上是本操作上限，不是完成时间预测。完整 validator/trace 下若驱动 CPU/ELU 超过既有质量门、出现调度迟到/丢弃、
请求或日志/采集预算耗尽，保留失败与 unknown，不通过关闭正确性或 trace 使负载看起来可达。
两组最大16臂共计划211200次 measurement 和52800次 warmup 请求；1MiB题每臂会解压约1.17GiB measurement 数据，
响应体不持久化进 trace。内存预算覆盖整个驱动，而不只并发响应缓冲，真实 OOM 仍判失败。

## validator / trace：已有自耗与尚不能分离的成本

现有 `http report` 可立即给出每题 measurement 的 `validationMs`、`decodeMs` 分布和 phase 的
CPU/ELU/event-loop delay。进一步从已核验、保持不可变的 trace 按 phase 求 `validationMs` 总和及完整响应数，
可得到每个完整响应的平均验证墙钟自耗；它包含 Buffer.concat、线上 body SHA、gzip 解压、UTF-8/JSON、
pointer/canonical hash 与比较，调度暂停也可能计入。`decodeMs` 是其中子集，不可再相加为总成本。

`run.evidenceWriteMs / Σ(run.phases.scheduled)` 是每条记录的平均 trace 生产墙钟自耗，包含 JSON 序列化、
Buffer 构造和同步 write；分母含 warmup+measurement、发送和未发送记录。该总数没有 phase 拆分，
不能只除 measurement 请求数。每个 run 最后的 fsync 在这个计时外；文件系统 page cache、后台写回、
存储持久化、最终 fsync 及争用成本不能由这个字段推出。不要对 P99 做“减去平均验证/写盘时间”的修正。

当前 raw 只存响应/检查摘要，没有响应体；公共 CLI 没有 validator/trace replay 或 off 开关。
若随后需要离线单位成本，可单独冻结有正确答案核验的有界响应 corpus，再在同一 Node/1CPU 配置重放对应阶段，
记录固定次数、总时间/CPU/字节与 setup/最终 fsync；这仍只是 corpus、缓存和文件系统条件下的单位成本。
用 reference 合成的 JSON 需标明 synthetic，不能声称等于真实响应 wire bytes、分块或缓存行为。
这一步不是当前交付新增实现，也不是本次两组成本实验的前置。

现有自耗、独立 collector 的 `collectorCpuNs/collector.processCpuNs/collector.peakRssBytes`、离线 replay 结果都不能
单独证明“去掉 validator/trace 后可多打多少负载”。collectorCpuNs 也未覆盖全部进程启动/事前 binary hash，
collector.processCpuNs 为截至摘要读取点的进程累计 CPU，仍不含后续封存和退出。整体施压能力继续由
**正确性和 trace 开启**的独立负载阶梯与驱动余量实验核验；
未做反事实实验的部分明确保持 unknown。两组观察成本结果只适用于已冻结的负载、日志 preset、采样频率和部署条件。
