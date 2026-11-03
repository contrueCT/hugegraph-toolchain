# LDBC v1 最小执行适配器

当前实现覆盖 IC1–IC14、IS1–IS7、IU1–IU8。官方 driver 负责生成、调度、依赖、短查询派生与计时，
Java 适配器负责 HTTP、结果契约和错误传播。Groovy 脚本以 HugeGraph 的固定 ID/schema 映射执行完整操作。
CLI 和这些组件可分别使用；通用任务调度与交互报告仍在阶段 2。

```text
官方 driver 8c6e849a...
  └─ HugeGraphDb：注册 29 类 handler
       └─ 有限线程 HTTP client：完整响应、字节/时间限制、禁止重试
            └─ /gremlin + 明确的 traversal alias
                 ├─ snb-read.groovy：14 复杂读 + 7 短读
                 └─ snb-update.groovy：8 更新、提交/回滚
```

脚本是正确性优先的基线实现。高阶邻域及全部最短路径可能消耗大量内存，不能将其视为 HugeGraph 的最优查询实现。
正式比较须冻结脚本、schema、索引和 jar；超时或资源不足使该次实验无效，不能截断返回结果来取得成绩。
IC13/IC14 使用双向逐层 BFS，完整保存最短路径前驱；IC14 返回所有最短路径及精确的半整数权重。
更新沿用原始方向；初始导入使用正 edgeKey，更新使用负的操作内序号，保留组织关系的多条记录。

## 构建与本地验收

要求 Java 17、Maven、Node 22.14+、Python 3.9+。下载源码使用 `gh`，固定仓库、commit、压缩包和全部 447 个文件的 hash。
源码压缩包约 1.7 MB，不包含图数据。构建官方 driver 会读取 Maven 缓存；首次联网构建会下载依赖。

```bash
python3 tools/fetch-ldbc-driver.py --out work/driver-source
python3 tools/build-ldbc-adapter.py --source work/driver-source --out work/adapter
# 缓存齐全时可加 --offline。输出目录必须不存在。
python3 tools/ldbc-full-contract.py --vectors-out work/unit-vectors.json
python3 tools/ldbc-driver-http-contract.py --build work/adapter --vectors work/unit-vectors.json
npm run check
```

`ldbc-full-contract.py` 使用手写单元图，覆盖全部操作及更新回滚，并以独立单向 BFS 核对 250 个路径结果。
HTTP 验收调用真正的官方 `DbValidator`，顺序执行 44 条记录；另测 12 类 HTTP/结果/未知提交异常，
以及6项独立JVM重试策略配置。后者在复用连接上模拟“更新已接收、响应丢失”，核对只发送一次并返回失败。
单元向量来自已有断言，不能代替独立参考数据库生成的 SF100 答案。

已有 HugeGraph 本地构建时，可以复用其 Surefire classpath，不再重新构建服务端：

```bash
python3 tools/ldbc-hugegraph-contract.py \
  --classpath-report /absolute/hugegraph/hugegraph-server/hugegraph-test/target/surefire-reports/TEST-CLASS.xml \
  --vectors work/unit-vectors.json --out work/hg-memory-check
```

这会在临时工作目录创建 HugeGraph memory 图、真实 schema，执行所有操作、核对脚本和回滚检查。
运行有 512 MiB 堆上限和 60 秒进程上限，完成后关闭图并删除临时工作目录。
证据记录实际 classpath 内容摘要。它没有启动 HStore，也没有经过 HugeGraph 的 HTTP 服务和鉴权层。

还可启动有时间和内存上限的本地 `ContextGremlinServer`，让官方验证器经真实 Gremlin HTTP 执行同一组单元记录：

```bash
python3 tools/ldbc-hugegraph-http-contract.py \
  --classpath-report /absolute/hugegraph/hugegraph-server/hugegraph-test/target/surefire-reports/TEST-CLASS.xml \
  --build work/adapter --vectors work/unit-vectors.json --out work/hg-http-check
```

服务只绑定本机临时端口，结束后停止。报告记录 channelizer、serializer、脚本安全模式及 SecurityManager 状态。
当前已验证配置为 memory 后端、GraphSON Untyped V1、legacy 脚本模式且未启用 SecurityManager；
没有验证生产鉴权、安全配置、REST 转发层、HStore 或 SF100。更新脚本显式返回单元素列表，
避免真实 Gremlin HTTP 将顶层 Map 展开为多条 entry；测试服务也采用相同的返回值契约。

## 运行官方 driver

构建目录包含 `ldbc-driver-standalone.jar`、`hgbench-ldbc-adapter.jar`、构建日志及 `build-manifest.json`。
不要混用其他版本的 driver 或在运行时解析可变 SNAPSHOT。正式运行前核对 manifest 中每个 jar 的 hash。

以下为 adapter 必需属性，加入由固定上游模板和 SF100 频率配置组成的 driver 配置：

```properties
database=org.apache.hugegraph.benchmark.ldbc.HugeGraphDb
workload=org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload
hgbench.endpoint=http://HOST:8080/gremlin
hgbench.alias=__g_DEFAULT-GRAPH
hgbench.timeoutMs=30000
hgbench.maxResponseBytes=67108864
hgbench.httpThreads=8
# 可选：环境变量保存完整 Authorization 值，配置文件只写变量名。
# hgbench.authEnv=HGBENCH_AUTHORIZATION
```

上面的 timeout、线程和响应字节数是配置示例，需在正式 profile 中定值和校准。
alias 必须取自服务端实际注册值；图空间命名可能含 `-`，例如 `__g_DEFAULT-hgbench_unit`。
它是请求中的别名映射目标，不会拼进 Groovy 脚本。
同一配置还须显式固定 `ldbc.snb.interactive.parameters_dir`、`updates_dir`、全部操作开关、
`operation_count`、`warmup`、`thread_count`、`time_compression_ratio`、随机种子及官方规定的其他参数。
SF100 频率取固定 driver 源码中的 `src/main/resources/configuration/ldbc/snb/interactive/sf100.properties`；
该文件只包含频率和 update interleave，不能单独作为完整运行配置。

```bash
java -Djdk.httpclient.disableRetryConnect=true -Djdk.httpclient.enableAllMethodRetry=false -Xmx2g \
  -cp 'work/adapter/*' org.ldbcouncil.snb.driver.Client \
  -P frozen-driver.properties -P frozen-sf100-frequencies.properties
```

用于正确性验证的配置须同时设置 `mode=validate_database`，并让 `validate_database` 指向独立参考实现生成的官方格式记录。
固定版本的 workload 初始化还需要实际 `parameters_dir`、全部 29 个操作 enable 键、
`scale_factor=100` 和 `short_read_dissipation`；它依据 scale factor 从固定 JAR 加载 SF100 频率。
验证模式按 CSV 重放，不生成或随机抽取操作。验证与测量使用独立配置和输出目录；每次从相同初始数据库状态开始。
SF100 的参数、更新和参考结果没有在本地伪造或替换为其他规模。

HTTP 适配器消费完整响应后才调用 `reporter.report`。它逐字段检查数值类型，拒绝重复 JSON key、字符串化整数、
不完整投影和错误提交回执。任意操作失败后，该次尝试停止提交新操作；已经发送的更新可能仍提交，须恢复状态后另起尝试。
不自动重试、不把 HTTP 200 等同于 Gremlin 成功。公开计时包含传输、完整解码、字段核对及官方结果对象转换的成本。

运行HG适配器须在JVM启动参数中显式设置 `-Djdk.httpclient.disableRetryConnect=true` 和
`-Djdk.httpclient.enableAllMethodRetry=false`。适配器在创建HTTP客户端前检查两项；后者为true、空值或未显式设置时拒绝启动。
JDK的全方法重试开关可以让已接收且丢失响应的POST在新连接上重发，单独关闭连接重试不足以约束这一行为。
严格有序入口也在初始化数据库前固定这两项；正式配置仍应记录完整启动argv，防止运行时属性变化或JDK内部缓存造成歧义。

## 严格有序验证入口

固定官方 `Client.main` 不检查 `ValidateDatabaseMode` 返回的 `DbValidationResult.isSuccessful()`，
因此错误答案仍可能让进程退出 0；缺少 handler 时，`*-failed-actual.json` 也可能是空数组。
`ValidateDatabaseMain` 是独立的小型入口，调用原 `Client.getClientModeFor` 和原验证模式，
将返回失败转为退出 4，要求输入完整覆盖 29 类并拒绝空文件。上游 driver 和验证实现保持原始字节。

```bash
python3 tools/build-ldbc-validation.py \
  --source work/driver-source --driver-build work/adapter --out work/validation-runtime

java -Xmx512m -Djdk.httpclient.disableRetryConnect=true -Djdk.httpclient.enableAllMethodRetry=false \
  -cp "$VALIDATION_RUNTIME/ldbc-validation.jar:$HG_ADAPTER/*" \
  org.apache.hugegraph.benchmark.ldbc.ValidateDatabaseMain \
  --out "$NEW_VALIDATION_ATTEMPT" -- \
  -P "$TARGET_VALIDATION_PROPERTIES" \
  -P adapters/ldbc-driver/validation/validation.properties
```

目标 properties 填写 `database`、连接设置、真实 SF100 的 `ldbc.snb.interactive.parameters_dir`
以及 `validate_database`；HG 的连接键沿用上一节。参考库若需自验，可在 classpath 使用固定 Cypher 运行包。
固定 driver 的多个 `-P` 参数以前面的文件为高优先级，因此目标配置放在基础配置前；
若目标配置写错模式，严格入口会在初始化数据库之前拒绝。
基础配置只用于 validate 模式，所有 29 个 enable 键已显式固定；生成/测量继续使用其各自完整配置和官方 `Client`。
固定 `Client` 的生成/测量种子是源码中的 42，不能把草案计划的通用 `seed=3346` 当作已传给 driver；
正式 LDBC profile 必须记录实际种子。图查询轨道的参数池种子 3346 单独保留。

入口先复制验证文件到新 attempt 目录，再让官方模式读取该副本，避免官方模式删除或覆盖原参考包旁的失败文件。
保留副本摘要、完整类型计数、原官方结果说明和 `receipt.json`；原始输入不变，已有 attempt 拒绝覆盖。
退出 0 / `validationSuccessful=true` 只说明这份有序答案匹配；输入来源、SF100 图身份、资源限制和恢复仍由外层控制器证明，
`referenceProvenanceVerified` 与 `performanceAccepted` 继续为 false。中途故障可能留下已提交更新，重试前必须恢复初始状态。
容器/进程组的 CPU、内存、磁盘、日志和总截止沿用外层执行控制，此入口不增加调度器。

专项使用真实官方 JVM 模式和无数据库协议替身；同一 Jar 在本机 JDK 21 与 Linux/JDK 17 均完成 44 条 / 29 类、12 项检查，
其中实际复现了官方错误答案退出 0、严格入口退出 4；公共 TS task 的成功、失败、复用与封存也已接通。
见接线及 Linux 原件（本机记录未随包分发）。没有运行小规模图数据或产生 SF100 成绩。

## 导入后逐分片核对

```text
冻结 Loader job + JSONL shard
                  |
可信的停写、排空、独占证明 + 目标 epoch
                  |
    全部身份 → 读取完整实际属性 → 精确比较
                  |
       observation.json + 小型逐批摘要
                  |
        hgbench benchmark recover
```

`tools/ldbc-reconcile.py` 是只读组件。它先调用 TS 控制器验证 job、请求和分片身份，随后按批扫描全部记录。
顶点核对 ID、label 和完整属性；边核对源点、目标点、label、edgeKey 和完整属性。
只有 emails/languages 以 SET 语义忽略顺序；仍拒绝重复值、类型变化和多余属性。64 位整数由 Python 精确处理。
TEXT 的首尾空白也参与精确比较。原生 Loader 的 trim 行为须在导入组件中修正；
离线小型覆盖 Jar 的构建、实际类来源检查和组件跟踪方式见[原始文本保留](sf100-data-pipeline.md#原始文本保留)。

读取方式可用 `--lookup-mode` 指定。默认 `traversal` 保持原有逐 ID 读取。
HugeGraph 1.8 大图可使用 `native-batch`：一次 `g.V(ids)` 或 `g.E(ids)` 批量读取，然后仍逐条核对身份和完整属性。
原生边 ID 使用 1.8 的 `ConditionQuery`/`EdgeId` 编码，适用于本适配器生成的独立边 label、唯一 `edgeKey` 排序键；
不兼容的 schema 直接失败，不自动切换算法。`--batch-size` 同时约束每个 HTTP 请求和原生 ID 批量大小。
报告记录实际 `lookupMode` 与所用脚本 SHA-256；旧模式的默认值、停写证明、输入摘要和错误退出规则保持。

每次重建/恢复使用独立图名与新 epoch。在应用 schema 的受控步骤中添加一个不参与图数据的属性键：
名称为 `hgb_epoch_` 加 `SHA256(epoch 的 UTF-8 字节)` 的前 32 位十六进制字符串，类型 TEXT。
核对器在每批检查图名及这个属性键。单纯留下旧 marker 不能证明恢复成功；恢复流程必须实际创建新图并隔离旧写入。

操作者须在扫描期间保证所有写入者停止、服务端在途写入排空、其他客户端无法写入。
Loader 退出码、进程消失或两次计数相同不足以证明这一点。使用以下外部证明文件：

```json
{
    "schemaVersion": 1,
    "kind": "sf100-writer-quiescence",
    "requestDigest": "<benchmark exchange 输出的 requestDigest>",
    "target": { "targetDigest": "<request.target.targetDigest>", "epoch": "<request.target.epoch>" },
    "graphName": "<job.config.target.graph>",
    "epochMarker": "hgb_epoch_<32 hex chars>",
    "validUntil": "<有时区的实际有效期截止时间>",
    "writersStopped": true,
    "inflightWritesDrained": true,
    "exclusiveAccess": true,
    "attestor": "<负责确认隔离的操作者或控制器身份>",
    "basis": "<实际隔离措施和可复核的排空证据位置>"
}
```

组件检查证明文件的请求/目标绑定和有效期，并保留原件；它无法替操作者证明停写声明属实。
没有可信证明时，不应把输出作为可重试或可跳过的依据。

```bash
python3 tools/ldbc-reconcile.py --job jobs/000000.json \
  --quiescence quiescence.json --alias GRAPH_g --endpoint http://HOST:8080/gremlin \
  --batch-size 256 --timeout-seconds 30 --max-response-bytes 8388608 --out checks/shard-000000
node dist/cli.js benchmark recover attempts/shard-000000/request.json \
  --observation checks/shard-000000/observation.json --artifact-root checks/shard-000000
```

固定 HugeGraph 1.8 的 SF100 环境可将核验命令配置为：

```bash
python3 tools/ldbc-reconcile.py --job jobs/000000.json \
  --quiescence quiescence.json --alias __g_DEFAULT-sf100 --endpoint http://HOST:8080/gremlin \
  --lookup-mode native-batch --batch-size 1024 --timeout-seconds 120 \
  --max-response-bytes 33554432 --out checks/shard-000000
```

批量模式拒绝未请求的 ID，重复 selector 的同一结果仍逐条计数；缺失、类型变化和文本裁剪继续阻止 committed。
其包路径与原生编码限于上述 1.8 基线，组件检查的结果见公开批量核验入口证据（本机记录未随包分发）。

边 selector 由 Python 同时传递 `sourceId`、`targetId`、精确 `edgeKey`，供原生 ID 构造使用。
可用兼容 HugeGraph 1.8 的现有 classpath 检查真实生产者与 Groovy 消费者的接线：

```bash
python3 tools/ldbc-reconcile-contract.py \
  --classpath-report /absolute/hugegraph/hugegraph-server/hugegraph-test/target/surefire-reports/TEST-CLASS.xml \
  --java-home /absolute/jdk-17 --out work/reconcile-contract
```

这项离线检查创建临时 memory 图，直接消费 Python 生成的边 selector，覆盖大整数排序键、重复和缺失边，
并在 HugeSecurityManager 下比较两种读取脚本；不启动服务或请求 HStore/SF100。
`--java-home` 可省略，输出目录须不存在；需要一致的服务端依赖，混合版本的本地构建不能充当有效基线。
修复及回归原件见边 selector 接线检查（本机记录未随包分发）。

endpoint 必须是冻结 Loader 目标的 `/gremlin`；可用 `--authorization-env` 引用鉴权环境变量。
工具拒绝重定向和重试，限制响应、批大小、单条 JSONL 和证据体积。退出 0 表示分片完整匹配；4 表示需处理的数据库状态。
HTTP 响应支持 identity/gzip，分别限制传输字节和解码后字节，保存原始 wire 摘要；JSON 长整数保持精确。
非 200 响应在同一个请求截止内最多读取 64 KiB（且不超过配置的响应上限），记录 HTTP 状态、可用的
Gremlin 数字状态码、已知异常类名及响应前缀摘要。正文、message、堆栈、响应头和请求内容不进入错误摘要。
错误正文超限、截断、无效或读取超时仍保留原 HTTP 失败；不重试。核验结果继续为 unknown / exit 4，
没有核验完整分片就不生成匹配计数。这是后续组件增量，不替换正在运行的 SF100 冻结版本。
损坏输入和前置条件错误直接失败。读取错误、证据超预算或 lease 失效会产生 unknown，不能据此重放写入。
证据保存每批请求/响应 hash、数量及总体结果，不保存第二份完整图数据；独立复核需要重新读取数据库。

分片核对覆盖该分片所有预期身份，无法发现分片身份集合之外的额外记录。
完成全部分片后，在同一停写/排空/独占条件下执行全图清单核对：

```bash
python3 tools/ldbc-inventory.py --job jobs/000000.json \
  --prepared prepared/prepare-manifest.json --prepared-sha256 REVIEWED_MANIFEST_SHA256 \
  --quiescence quiescence.json --alias __g_DEFAULT-GRAPH --endpoint http://HOST:8080/gremlin \
  --timeout-seconds 30 --page-size 100000 --max-pages 100000 --out checks/initial-inventory
```

转换器会生成并锁定 `inventory.expected.json`；旧的转换产物须重新生成，不能手填一个“通过”的清单。
全图核对同时绑定转换清单 hash、Loader 的 mapping 身份、目标 epoch 和外部独占证明。
它逐条扫描顶点、边各一次，按实际 label 累计数量；不将按 label 下推或聚合 COUNT 当作验收依据。
扫描不在脚本中保存整图或全部 ID，但会占用数据库的读取、解码、事务及缓存资源。
这里只核对 HugeGraph 公共图数据/schema；内部隐藏系统 label 不在 LDBC 模型范围内。

默认使用后端分页，`--timeout-seconds` 限制每个请求，`--page-size` 范围为 1–100000（默认 10000），
`--max-pages` 限制每种元素的页数。每页核对目标/epoch、扫描上限、计数与游标，扫描前后再次核对 schema。
页证据保存为 `pages.jsonl`，最终清单包含其长度和 SHA-256；重复游标、读取失败、预算不足或租约过期均为 unknown，
不重试、不把部分扫描当作全图成功。`--legacy-single-request` 显式保留旧路径，不适合 SF100 的请求截止时间。

上面的 SF100 页大小只是待实际单页成本核验的执行配置，不是性能校准结果。全图约 20.6 亿个元素，
页大小 10000 会产生至少约 20.6 万页；页大小 100000 则约 2.06 万页。导入规划中的 `maxLogBytes` 同时冻结了
核对证据预算（上限 64 MiB），必须在执行前为完整页账本和其他清单预留容量；该预算不会因增加页数自动扩张。
有效停写租约和外层执行截止必须覆盖整个扫描，不能仅设置单个请求的 timeout。

schema 核对包含属性类型/基数/写入类型、顶点 ID 策略、属性与 nullable 集合、边方向/多重性/排序键、
TTL、label index 开关，以及完整的自定义索引清单。默认转换不创建自定义索引；新增索引须进入新的冻结映射。
清单额外核对每个 label 的全体分片记录总数与 CSV 计数，拒绝混用转换结果。

输出为 matched / mismatch / unknown；`contentChecked:false` 明确表示它须与逐分片内容证明组合使用。
超时或中断不能得到匹配结论，不能用部分扫描当作全图数量。完整 SF100 扫描属于重资源任务；
实际 timeout、服务端执行上限和停写有效期须按确认的资源窗口设置，完成后再恢复相同起始状态进入正式测量。

## 第一阶段剩余验收

1. 确认测试集群、磁盘和资源窗口，冻结 SF100 实际文件及转换后摘要。
2. 真实 HStore 导入、全分片及全图计数/schema 核对；独立参考实现生成有序答案，官方 driver 验证。
3. 冻结环境和测量参数，做 A/A、已知退化、负载阶梯与驱动余量校准。
4. 在另一环境或由另一使用者独立复现，审阅版本化发布候选。

上述步骤仍属于阶段 1。单元验证通过不会将阶段 1 或正式 LDBC 成绩标成完成。

## 独立参考实现构建

参考输入桥接、固定 Cypher 源码及官方 driver 的显式绑定见 [参考实现接入](../adapters/ldbc-reference/README.md)。
当前已完成本地编译和 29 类 Java 接口校验，参考库实际执行和 SF100 有序答案仍未生成。
