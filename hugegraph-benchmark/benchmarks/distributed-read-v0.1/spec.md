# HG-Bench Distributed Read v0.1-draft

本套件是 HG-Bench 的主性能方向，面向 HugeGraph Server + PD + Store 的分布式大图。
当前状态：本地可复现的规范与生成器候选，性能参数待专用环境校准。Core Read 小图保留为语义检查集。

## 1. 数据模型与规模

生成器流式输出顶点、边和校验参数，不把整张图装入内存，也不使用互不相连的小图副本扩容。
所有顶点属于同一张有向强连通图。环边连接全图；稀疏 hub 集合吸收倾斜入流量并具有较大出度；
其余边通过固定整数算法分散到全局 ID 空间。固定 seed 为 3346，ID 为 1 到 N 的数字。

| 档位 | 顶点 | 有向边 | 每顶点 payload | 用途 |
| --- | ---: | ---: | ---: | --- |
| dev | 8,192 | 73,664 | 256 B | 本地协议、生成器与独立 oracle 验证 |
| s1 | 1,048,576 | 9,428,992 | 256 B | 分布式 pilot 起步 |
| s2 | 16,777,216 | 150,863,872 | 256 B | 中大型图 |
| s3 | 134,217,728 | 1,206,910,976 | 256 B | 亿级顶点、十亿级边候选 |

每 1024 个顶点有一个出度 1024 的 hub，其余顶点出度 8。无自环、无平行边。
顶点属性为 group、rank、payload；group 是 4096 个等大小分组之一，rank 等于 ID。
payload 由四个带 block 编号的 SHA-256 十六进制字符串组成，不在不同顶点间重复使用同一常量。
它仍是合成文本，压缩率和业务代表性必须在导入报告中披露。

这组分布是可解释的合成压力模型，不声称服从真实业务幂律。后续加入版本固定的 LDBC 数据及业务脱敏分布，
各自使用独立数据集身份，不用相同规模名掩盖不同拓扑。来源方法参考 [FinBench](https://ldbcouncil.org/benchmarks/finbench/)
和 [SNB 的分规模数据与参数组织](https://ldbcouncil.org/benchmarks/snb/datasets/)，本套件不声称符合其完整标准。

## 2. 大图、工作集与分区

规模名字不直接决定“磁盘受限”。必须同时披露逻辑数据、去除副本后的物理数据、含副本总占用、
所有节点 JVM/应用缓存、RocksDB block cache 和 OS page cache 的预算及实际观测。
`warm-sampled` 与 `storage-sweep` 分开报告。后者要求唯一数据量至少是聚合缓存预算的两倍、
本 case 相关源域完整访问一轮，并有 Store 物理读证据；若任一条件不足，标记 `unproven-storage-sweep`。
不能通过填充大量从未访问的数据把一个小热点实验命名为大图磁盘实验。

```text
负载机 -> Server 实例 -> PD 路由与元信息
                   └-> Store 分区 A/B/C/... -> Raft 副本与 RocksDB
                        ↑ 实测 leader、分区访问量、跨节点流量
```

默认 pilot 拓扑为 1 个独立负载节点、1 个 Server、3 个 PD、3 个 Store，副本数 3、逻辑分区 24。
正式运行须提交实际配置。角色可以按已披露方式共置；不同 Store 故障域及负载机必须独立。
单机多进程只作开发验收。扩容实验使用 Store 数 3/6/12，分别进行固定总数据的强扩展及固定每节点数据的弱扩展，
两类实验分开；变更副本、分区数或 Server 数会形成新的比较条件。

生成器不假设 `ID % partitionCount` 就是 HStore 分区。导入后记录 PD 分区/leader 映射和每个 Store 的实际流量。
跨分区遍历需要至少两个实际存储节点参与同一逻辑请求的证据；全图分散或多个节点有总体流量均不足以证明这一点。
若当前版本无逐请求追踪，保留该项 unknown，不能给出跨分区收益归因。

## 3. 固定任务

所有边方向为 out，label 为 link；不隐含 degree/capacity 截断。任务的集合、完整属性、分页和最短路径
正确性规则沿用 Core Read 的语义定义。传输入口需披露，不合并不同入口成绩。

| 题目 | 操作 | 压力与正确性目标 |
| --- | --- | --- |
| DR01 | 均匀 ID 命中，完整顶点 | 点查、属性读取 |
| DR02 | N 以外的 ID 未命中 | 正确区分不存在与系统失败 |
| DR03 | group 等值索引，完整 ID 集合 | 索引访问与随规模增长的结果集合 |
| DR04 | 普通顶点一跳 | 8 个不同邻居 |
| DR05 | hub 一跳 | 1024 个不同邻居 |
| DR06 | 普通起点 nearest Kout 两跳 | hub 扩张、去重与完整返回 |
| DR07 | hub 起点 nearest Kout 两跳 | 高扩张、跨分区候选路径 |
| DR08 | DR07 的 final-layer limit 64 | 返回任意合法 64 个不同成员，不截断中间层 |
| DR09 | hub 邻居分页，每页最多 128 | 完整消费全部页，禁止客户端切分一次性结果 |
| DR10 | 两个相邻 ID 的共同出邻居 | 精确交集，允许空集 |
| DR11 | 两跳环路径的最短路径，maxDepth 4 | 必须是合法最短路径；如有直接边则长度必须为 1 |
| DR12 | group 的完整顶点物化 | s3 每次返回 32,768 个完整顶点，约 8 MiB 纯 payload |

`validation.ndjson` 每题 16 组，共 192 组，供适配器导入前后交叉验证。
性能运行从 `parameterAt(caseId, ordinal, scale)` 生成确定性连续参数流，不能循环这 192 组校验题。
源 ID 使用置换遍历，hub 和 group 域分别遍历；普通顶点域使用 cycle walking 排除 hub。
每题的 ordinal 独立，从 0 开始；一个并发调度器原子分配 ordinal，派发顺序写入证据。
默认是均匀访问加图本身的结构热点。额外热点访问比例、幂律拓扑、业务混合必须另建 profile。

## 4. 测量协议候选

机器可读完整参数见 `performance-profile.json`：并发 1/8/32 为三个独立 track，closed-loop，
预热 120 秒、测量 300 秒、最多 drain 30 秒、每次逻辑操作 30 秒超时，5 个独立区组。
每题分别运行，完成全部页和响应解码后停止计时。重试关闭；错误、超时和未完成都进入分母。
闭环结果不能表示固定到达率下的线上服务容量；开放到达率及容量阈值留给独立 Serving 套件。

这些数值是预先声明的 pilot 候选，不是已经校准的经验结论。修改任何数值生成新 contract digest，
不能看完 A/B 结果后择优修改。p99 样本不足 10,000 时只标记不足；低于 1,000 个完成样本的 cell 不给比较结论。
预热不稳定、错误率非零、关键监控缺失都使相关成绩无效；保留全部原始证据，不自动重跑到满意为止。

只读静态图也可能发生 compaction、flush、snapshot、GC 与副本活动，必须按 1 秒粒度记录。
稳定态没有计划写入和 rebalance；后台活动不静默过滤。持续导入、压实压力、分区迁移、leader 切换与故障恢复
属于独立的生命周期赛道，后续定义固定扰动脚本和恢复指标，避免它们随机混入只读基线。

单项指标为成功完成吞吐、p50/p95/p99、错误与资源开销；没有总分。
A/B 以独立区组为统计单元，区组顺序固定在 profile 中。bootstrap 区间仅作描述，
专用环境完成 A/A、已知退化、负载机与采集器余量验证前，自动 regression/pass 门禁关闭。

## 5. 本地复现与证据

```bash
node benchmarks/distributed-read-v0.1/generator.mjs --help
node benchmarks/distributed-read-v0.1/generator.mjs describe s3
node benchmarks/distributed-read-v0.1/generator.mjs generate dev .hgbench/distributed-dev
python3 tools/verify-distributed.py .hgbench/distributed-dev
```

生成目录必须不存在；中断目录保留供排查，重跑使用新目录。先检查磁盘预算再显式生成 s1/s2/s3。
默认不下载数据、不联网、不生成大规模资产。输出 manifest 记录规则 hash、每个文件的 SHA-256、行数与字节数。
SQLite 验证器独立读取实际文件并运行查询，不调用生成器邻接函数，按磁盘索引完成校验。
`expected()` 是低内存的生成模型 oracle，仅用于离线验证，禁止进入被测查询实现。

生成器、独立 oracle 和适配器验证通过，只能证明本地数据与协议闭环。
HStore 校准、分区流量、完整大规模生成/导入和独立环境复现均需单独证据。
发布 gate 保留这些状态，环境补齐前交付的版本保持 draft。
