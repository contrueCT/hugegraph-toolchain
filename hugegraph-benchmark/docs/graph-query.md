# 图查询 quick / standard v0.1

本入口复用 TS 调度、Go HTTP 执行器、任务生命周期、离线分析及 SVG 报告；无新增服务、前端或依赖。两档都覆盖 SF100 上的五类查询，使用完全相同的数据、参数池与独立参考。本文描述工具协议；真实 SF100 正在导入，尚未完成全图核验或运行默认图查询两档。已有功能联验使用固定响应协议服务，没有加载或测量小图；验收范围及未完成项见[阶段一验收边界](phase1-acceptance.md)。

```text
官方 SF100 独立参考 CSV ── SHA/行数核验 ── SQLite Person/knows 索引
                                                │
                                分层参数 + 精确独立答案 + 内容摘要
                                                │
已有 Go profile ── graph prepare ── 冻结 GET/POST 请求池
                                                │
                    图专用代表校准/缓存 → 并发阶梯 → 新窗口复测
                                                │
                            raw → 独立语义重验 → HTML/CSV/SVG
```

## 准备一次

参考构建只读取已转换的完整 SF100 独立 CSV，逐字节核对 Person 与 knows 文件的 SHA、行数、大小、ID、方向、重复边和悬空端点。完整图 manifest 必须声明 282,637,871 顶点及 1,775,513,741 边，并与同一官方 SF100 初始资产绑定。索引只覆盖 Person/knows 投影；这不能替代完整图导入核对，也不产生官方 LDBC SNB 成绩。

```sh
hgbench graph reference index \
  --manifest /reference/reference-manifest.json --data /reference \
  --hg-manifest /converted/prepare-manifest.json --out /new-reference-index
hgbench graph reference export \
  --index /new-reference-index --out /new-reference.json \
  --seed 3346 --pool-size 128
hgbench graph prepare /new-reference.json --profile /current-go-profile.json \
  --graph-url http://host:8080/graphspaces/DEFAULT/graphs/sf100 \
  --traversal-source __g_sf100 --out /new-graph-profile.json
```

索引/导出默认整任务上限 7200 秒、独占输出上限 8 GiB、剩余磁盘下限 80 GiB；这些可显式配置。SQLite cache 64 MiB，临时索引文件放入自有输出目录。正式远端准备放入有 CPU/内存/磁盘保护的 Docker 环境；当前 CLI 不自动部署数据库。保护为定期检查和 SQLite progress handler，非文件系统硬配额。输出目录/文件必须是新的；失败索引保留 failure.json，缺少完整 index.json 时拒绝导出，无隐式续跑或重建。实际 SF100 首次索引在反向排序时暴露 SQLite 3.40.1 缓存临时目录：晚设 `SQLITE_TMPDIR` 仍落到容器 `/tmp`。
当前源码为单进程、单连接独立工具，使用并检查 `temp_store_directory` 显式绑定，并统计 Linux 已 unlink 但仍打开的临时文件；
SQLite 无法确认绑定时直接失败。该 SQLite 设置为进程全局，不支持把此模块嵌入并发、不同临时目录的 SQLite 工作负载。
macOS 检查 3 通过 / 1 Linux 专项跳过；同 Linux 镜像四项全通过。远端独立参考已完成，索引和五题参数/答案已验收，见已接受的参考原件（本机记录未随包分发）；它只证明独立 Person/knows 投影，目标完整图核验仍待完成。
此修正使用新冻结脚本接回，没有覆盖旧交付包或旧失败输出。

默认每题最多 128 个参数。对全 Person 人口按出度 0、1–10、11–100、>100 分层作固定种子 reservoir，再轮转非空层；点查每第十个参数为明确的缺失命名空间。共同邻居包含随机对及共享后继候选，路径包含有限随机游走候选及随机对。交集空/非空、最短距离/不可达的实际分布记录在参数池和报告中；不承诺不存在的层或固定命中比例。种子、顺序、权重规则、人口与参数摘要全部保留。同环境比较须复用同一份冻结 reference，不能每次重新抽参数。

独立路径参考用有向邻接与反向邻接精确计算 1/2/3 跳最短路径，保存所有最短路径的边见证。搜索超预算、结果超过 100000 边或导出大于 3 MiB时明确失败；不会把未完成搜索标作不可达。`--max-visited` 默认 1000000，约束已读端点及二跳边的搜索工作量。修改池大小/预算需要在新导出前明确设置，工具不会自动缩减题目。

## 数据完整度

允许在同一 SF100 源数据的部分导入快照上运行两档查询。导入调度设置
`minimumEdgeCoverage: 0.95`，在完成一个分片、收到成功写入确认后判断记录数比例；
设置为 `1` 要求100%。已确认分片保持，未知写入结果仍停止处理，不能算入已确认记录。
`tools/ldbc-import-sequence.mjs` 的 `finalVerification: "write-acknowledgements"`
启用这条轻量路径；原 `full-content` 路径继续提供完整内容核验。

完成导入后固定同一快照，停止写入，再生成查询 profile：

```sh
hgbench graph prepare reference.json --profile go.json \
  --graph-url http://localhost:8080/graphspaces/DEFAULT/graphs/sf100 \
  --traversal-source __g_DEFAULT-sf100 --import-snapshot snapshot.json \
  --min-edge-coverage 0.95 --out graph.json
```

`--min-edge-coverage 1` 选择100%。`hgbench schema graph-import-snapshot`可查看快照格式。快照记录源清单摘要、已确认写入清单摘要、
顶点与边计数、每种标签的已导入/预期/缺失数量及内容核验状态。计数来自 Loader
成功回执时，报告明确标注该依据，不宣称逐条内容已核验。源SF100计数和目标实际导入数分别保存；
快照进入 profile 摘要、原始证据和离线报告，跨版本比较应使用相同快照。

本版五题依赖 `person_knows_person` 的完整邻接关系，导入调度可先完成这类边，
再导入其余背景数据直至所选总比例。查询仍逐条检查独立参考答案；数据完整度阈值不放宽答案规则。
没有附带快照的旧profile保持原行为，报告不把目标完整度标为已验证。

Loader 配置支持 `checkVertex`（未设置时为true）。顶点及标签映射已有核验依据时可显式设置false，
避免每条边重复读取两个端点；`batchSize`、`batchThreads`、`parserThreads`继续使用现有配置项。

## 查询语义

| case              | 请求和结果判定                                                                                                                                          |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| point             | 原生 GET 点查；命中校验字符串 ID 与 person 标签，缺失校验 404 和 NotFoundException/vertex 消息。完整属性随响应解码，但当前只独立校验身份及标签。        |
| neighbors         | kout，OUT/person_knows_person、深度 1、nearest=true、max_degree=-1、capacity=-1、limit=100；无序、唯一、来自完整独立邻居集，数量必须为 min(100, 度数)。 |
| common            | sameneighbors，两个端点的共同 OUT 邻居；max_degree=-1、limit=100，同样校验完整交集中的无序唯一子集与数量。                                              |
| shortest          | shortestpath，OUT、最大深度 3、max_degree=-1、skip_degree=0、capacity=-1；接受任一同长最短路径，逐边与独立见证核对，排除反向、绕路、重复点和假不可达。  |
| gremlin-neighbors | bindings 传字符串 userId，显式 traversal-source；`g.V(userId).out('person_knows_person').id().limit(100)`，校验 `/result/data` 的同样集合语义。         |

所有 ID 保持字符串，避免大整数经过 JS 浮点转换。邻接索引拒绝重复有向 knows 边和自环，以固定本版 Gremlin 去重及 kout 排除起点的语义；有这些数据时应定义新的显式题集协议。REST URL 保留调用者的 graphspace/上下文路径；Gremlin alias 必须提供实际值。五题各自报告，不合成总分。HTTP 调用包含接口/序列化/网络成本，不能解释成纯存储引擎耗时。

## 执行

```sh
hgbench graph describe
hgbench graph run /new-graph-profile.json --preset quick --out /new-quick --dry-run
hgbench graph run /new-graph-profile.json --preset quick --out /new-quick
hgbench graph run /new-graph-profile.json --preset standard --out /new-standard
hgbench graph report /new-standard --out /new-offline-report
hgbench schema command:graph.run
```

| 默认                | quick                | standard                      |
| ------------------- | -------------------- | ----------------------------- |
| 并发探索            | 1 / 4 / 16           | 1 / 2 / 4 / 8 / 16 / 32       |
| 探索每点预热 / 测量 | 5s / 15s             | 10s / 20s                     |
| 新窗口复测          | 候选点一次，5s / 60s | 候选及邻近点各三轮，15s / 60s |
| 固定窗口上限        | 625s / 10分25秒      | 3150s / 52分30秒              |

另计首次代表校准 80 秒、前后检查、进程准备、排空、分析和收尾。`--settings` 提供事前自定义值；短窗功能联验不算默认 quick/standard 大图实测。默认复用现有图 JVM，复测为新的时间窗口与 driver 进程。每阶段从参数序号 0 开始，按 sequence % poolSize 轮换；闭环不同 worker 共享序号，实际完成顺序允许变化，报告每窗覆盖率、分层发送数、最少/最多访问次数。慢题未覆盖的参数保留缺口。

排空及在途数按客户端请求记账。客户端超时或取消后，数据库后台查询是否已停止仍需目标侧证据；驱动在途数归零不能证明目标空闲。预热或测量有请求错误的负载点不参与候选选择，后续窗口也不能仅凭客户端排空取得正式性能资格；目标窗口资格继续按下文的 unknown 边界解释。

`--budget-ms` 默认整次硬上限；`--budget-mode stage-sum` 要求保守阶段预算。保留部分结果/缺项，不自动缩短窗口或减少题目。SIGINT/SIGTERM 取消原子任务并收尾；断线后从 state.json 的 activeTask 查原任务，不能重复派发同一输出。请求最多 100 万/阶段，原 trace 最多 1 GiB/子任务、8 GiB/整次，默认剩余空间下限 2 GiB；磁盘/请求/trace 上限使结果受限，不能当作数据库极限。

## 图专用轻量校准及报告

首次自动选点查与非点查中参考响应最大的题，分别 1000/40 rps、IF64、10 秒预热 + 30 秒测量。provider 根据同一冻结参数池构造正确响应，校准覆盖参数选择、实际所选请求类型、对应语义校验和写证据；不访问 HG。缓存独立绑定图池、参考、二进制、机器、编码、连接和校验实现。HTTP 固定题校准不能直接替代。`graph calibrate` 单独执行/复用，`--refresh-calibration` 显式更新。

代表响应只包含其独立参考所需投影，特别是点查不会伪造完整业务属性。实际响应超过校准大小/速率、其他 case 或驱动出现压力时标记未覆盖/受限。校准结果和实际逐点健康分别记录，完整 limited 记录可复用，保持原限制；报告只称最高观察正确吞吐及复测范围。

raw 逐条保存参数序号、实际语义投影与状态、时间、字节和资源；离线 reader 重新检查这些值及冻结请求/数据/参考摘要，不信任 runner 的 ok 字段。集合只保存至多 100 个实际 ID，最短路径至多 4 个，点查保存身份/标签或缺失信封。报告提供分题吞吐曲线、延迟、错误和参数覆盖，每项 SVG 可独立导出。HTTP/图查询和不同数据/参数定义的单元保持分开。

正式实测前需达到所选导入完整度并固定数据快照，并关闭 HG memory monitor 触发手动 GC（受管环境为 threshold=1）与宿主 swap，采证覆盖实测窗口。当前便利入口未自动关联目标逐窗 GC/swap 或全图身份；报告为 unknown，performanceAccepted=false。当前协议功能通过不能提升为 SF100 性能资格或阶段一完整验收。

## 可选的已有 GC / swap 日志诊断

等待 SF100 导入时新增的离线入口只读取已有文件，复用原分析和报告，不启动监控服务或更改目标 JVM：

```sh
hgbench graph report /graph-standard --out /new-report \
  --target-diagnostics /observations/manifest.json
# HTTP 同一调度的报告也支持：
hgbench http throughput report /http-standard --out /new-http-report \
  --target-diagnostics /observations/manifest.json
```

manifest 绑定原 attempt `state.json` 的 `planSha256`。`path` 相对 manifest 所在目录，大小为字节整数、摘要为原文件 SHA-256：

```json
{
    "schemaVersion": 1,
    "kind": "throughput-target-diagnostics",
    "planSha256": "<original attempt plan SHA-256>",
    "maxSampleGapMs": 10000,
    "hostSamples": {
        "path": "graph-host-samples.ndjson",
        "bytes": 1234,
        "sha256": "<host sample file SHA-256>",
        "format": "sf100-host-guard-v1"
    },
    "gcLogs": [
        {
            "role": "server",
            "path": "gc.log",
            "bytes": 5678,
            "sha256": "<GC log SHA-256>"
        }
    ]
}
```

`hostSamples:null` 或 `gcLogs:[]` 明确保留缺失项。支持当前 SF100 host guard、已有 HTTP standalone observer 及被动 resource collector 的原生 NDJSON；format 分别为 `sf100-host-guard-v1`、`http-standalone-observer-v1`、`http-resource-collector-v1`。只读取这些格式的原始 `SwapTotal`，字段缺失不按 0 处理。采样须包围整个固定预热/测量窗，超过声明采样间隔、缺指标或覆盖不足时为 unknown；看到非零时明确展示。日志目前读取 HotSpot 的 `time,uptime,level,tags` 格式，例如 `[2026-10-28T07:00:00.000+0000][1.000s][info][gc] GC(1) Pause Full ... 100.000ms`；未知或截断 pause 行计入限制，缺日志不表示无 GC。

每份文件最多 32 MiB，附件合计 64 MiB、最多 16 个 GC 文件；不对超限文件自动截断。摘要、字节或 plan 不匹配时拒绝附件。报告复制原 manifest 和原始输入到 `target-diagnostics/`，移动后可用该 manifest 再次离线重建。当前实现按 Go `startedAt` 和单调阶段偏移作近似 UTC 关联；`startedAt` 并非精确 origin，目标身份、主机时钟及日志完整采集也未独立验证。因此只展示诊断线索，不认定 GC 导致慢点，不排除样本，不改变选点/指标/质量门；正式逐窗 GC/swap 资格仍为 unknown、`performanceAccepted=false`。当前远端冻结执行包保持，完整 SF100 实测尚未完成。
