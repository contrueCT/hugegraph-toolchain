# SF100 数据转换与 Loader 接入

数据路径现在可以从经过校验的 CsvBasic 输入，直接生成分片及 Loader 单分片任务。
本页记录流水线实现；远端 SF100 获取与部署状态见 [阶段一验收边界](phase1-acceptance.md)。
真实SF100的4,449份转换分片及完整输入证明已验收，目标为282,637,871顶点/1,775,513,741边；见输入回执（本机记录未随包分发）。当前HStore全量导入/数据核验仍在运行，输入完整性不代表数据库已完整导入。

官方initial内嵌更新流和选定standalone更新流的全量审计也已完成，无需重新展开或扫描35.53GB归档。person流字节相同；forum流仅在相同scheduled timestamp组内次序不同，组内完整行/载荷摘要、操作数和时间范围相同。实际执行继续使用原standalone字节顺序，不能从组摘要推断并列操作独立或数据库重放正确。终态复核（本机记录未随包分发）核对了旧服务MainPID0/成功及原件摘要，未重新读取大包；真实更新引用和完整29操作有序验证仍待验收。

```text
官方资产锁 + 归档成员清单 + 原压缩包（也兼容已展开目录）
          |
 Python 映射、身份/端点检查
          |
 不可变 JSONL / JSONL.GZ 分片 + schema + struct + manifest
          |
 TS loader-plan：冻结每片输入、目标与 Loader 程序身份
          |
 loader-run：默认预览；显式执行后保存请求、日志与未知结果回执
          |
 写入确认 -> 顶点内容门 -> 边写入确认 -> 全图内容/schema/index清单
          |
 未知写入效果 -> 原逐片读回核对 -> 分类后恢复，保持原写入身份
```

## 分片转换

推荐入口直接消费冻结归档，默认输出 GZIP 分片：

```bash
python3 tools/ldbc-prepare-archive.py \
  --catalog "$CATALOG" --inventory "$INITIAL_INVENTORY" --cache "$CACHE" \
  --out "$NEW_PREPARED_OUTPUT" --compression gzip \
  --shard-rows 500000 --shard-bytes 67108864 \
  --max-data-bytes "$EXPANDED_JSONL_BUDGET_BYTES" --max-stored-bytes "$STORED_SHARD_BUDGET_BYTES" \
  --max-index-bytes "$INDEX_BUDGET_BYTES" --max-total-bytes "$TOTAL_OUTPUT_BUDGET_BYTES" \
  --reserve-free-bytes "$FREE_SPACE_RESERVE_BYTES" --max-seconds "$APPROVED_SECONDS" --plan
```

`--plan` 只检查小型 catalog/清单、显示两遍输入成本和资源边界，不读取数据归档或创建输出目录；它不保证缓存文件或输出权限已就绪。
确认重任务预算后去掉 `--plan` 才执行。中断恢复使用相同身份/预算及 `--resume`；完整结果会重查输出，未完成结果会重建索引并重放归档。
两遍共读取约 52.31 GB 压缩字节：先建顶点唯一性索引和 Person 数组，再逐批核对边端点。
每遍都验证压缩 SHA-256/长度和所有成员，转换处理全部图表字段与行数。

初始归档内嵌的更新 CSV 会核对摘要但不作为初始图导入；正式更新使用 catalog 的独立更新包。
分区文件在 tar 中乱序时，edgeKey 仍按排序后的成员路径及行偏移分配，保持与旧映射相同的边身份。
空表也生成完整 schema 属性。Person 暂存行/多值属性和全部顶点身份使用紧凑 SQLite，边端点按最多一万行一批核对。
临时索引始终可重建，成功后移除；不保存原始 CSV 或全图 JSON 副本。

`--max-total-bytes` 覆盖当前输出目录的分片、索引、元数据和失败记录；目录大小每 5 秒、剩余空间每 0.2 秒检查，
属于轮询保护，仍需外层 cgroup/服务截止与文件系统余量。输入缓存和未来数据库不计入该目录额度。
源码链、catalog/清单、压缩器版本及预算参与恢复身份，修改后需使用新目录。
`sourceConverterSha256` 在此入口表示带末尾换行的 canonical JSON 源码文件摘要映射的 SHA-256，manifest 明示计算方式和组成文件。

已拥有展开输入时仍可使用原入口；两种命令均须事先确定真实环境预算：

```bash
python3 tools/ldbc-prepare.py --catalog sf100.lock.json prepare \
  --initial sf100-extracted --out sf100-prepared \
  --shard-rows 500000 --shard-bytes 67108864 \
  --compression gzip --max-data-bytes "$EXPANDED_JSONL_BUDGET_BYTES" \
  --max-stored-bytes "$STORED_SHARD_BUDGET_BYTES" --max-index-bytes "$INDEX_BUDGET_BYTES"
```

每片同时受记录数和字节数限制；单条记录过大时失败，不切断 JSON 记录。
分片文件先写 `.partial`，同步后排他发布，原文件不可覆盖。只有全部映射与完整性检查完成才发布 `prepare-manifest.json`。
manifest 记录每片的稳定 ID、类型、标签、记录数、大小和 SHA-256。
`--shard-bytes` 和 `--max-data-bytes` 约束解压后的 JSONL；`--max-stored-bytes` 约束磁盘分片总字节，包含压缩头和尾部。
未指定存储上限时沿用 `--max-data-bytes`，未指定压缩时沿用普通 JSONL。

GZIP 使用 level 1、空文件名和固定时间戳；压缩器运行时版本参与恢复身份校验。
压缩准备清单和 Loader job 使用 `schemaVersion: 2`，普通 JSONL 保留 v1。旧版 CLI 会在 schema 检查时拒绝 v2，
避免忽略压缩字段后继续执行。通用 SF100 请求/回执协议仍为 v1，压缩文件身份继续绑定在请求中。
每片同时记录压缩文件的 bytes/SHA-256 与 `encoding.uncompressedBytes/uncompressedSha256`，
schema 和点边逻辑内容保持一致。Loader 原生 mapping 使用 `compression: "GZIP"`，CLI 完整传递此字段。
规划和核对会检查 mapping、encoding 与准备清单的绑定；只标记压缩却缺少展开身份时拒绝执行。
分片核对直接流式解压，校验 gzip CRC、双重长度/hash、记录数和逐记录完整内容，展开字节超额即失败。
普通和压缩分片均走同一套 committed/absent/partial 恢复规则；进程成功依然需要核对。
转换发布直接复用写入时计算并封存的分片摘要，避免再通读一次全部分片；Loader 提交前仍独立校验磁盘文件。

新模式将 SQLite 用于全局顶点 ID、多值属性关联和最多一万条边的批量核对，不再保存全部点边 JSON 内容。
边端点同时核对 ID 和 label，避免共享 Message ID 编码掩盖 Post/Comment 类型错误。
SQLite 主文件用 `max_page_count` 限制，缓存上限设为 64 MiB；JSONL 累计字节另有预算。
分片和索引上限不包含输入文件、SQLite 日志/临时文件、文件系统分配开销、元数据和数据库占用，部署仍需保留额外磁盘余量。
原 `ldbc-prepare.py` 命令要求已展开的 CSV 输入；`ldbc-prepare-archive.py` 使用上面的两遍归档协议。
真实SF100转换与完整输入验收已经完成；转换产物的空间证明不包含未来数据库大小。
完整目标导入、扫描及运行峰值仍须由实际执行证据验收。

中断后使用相同参数和 `--resume`。转换器固定输入清单、catalog、代码身份和分片/预算设置，变化时要求新目录。
恢复会重建临时索引并从输入起点重放；已封存的分片须与重新生成的内容完全一致才复用，部分文件重写。
因此它节省重复输出写入，并不消除重新解析输入和建立索引的时间。当前还没有 CSV 偏移级恢复或分布式转换执行器。
完成后的再次调用会校验输出清单；不会把损坏文件当作缓存成功。
正常取消释放单写入者锁。强制杀进程留下的 `.conversion.lock` 需先确认原进程已停止，再人工处理。

不指定 `--shard-rows` 时保留原有小规模参考转换行为。旧版 SQLite 全量答案 oracle 与新紧凑索引用途不同。

## Loader 任务规划

```bash
node dist/cli.js schema sf100-loader-config
node dist/cli.js benchmark loader-plan sf100-prepared/prepare-manifest.json \
  --config loader-config.json --out jobs
node dist/cli.js benchmark loader-run jobs/000000.json --out attempts/shard-000000
```

最后一个命令默认只显示 argv、JVM 堆设置和前置条件。规划读取元数据，不扫描全部图数据、不运行组件。
`loader-plan` 默认输出任务数量与产物位置；`--detail full` 才返回全部任务。

配置引用已有 `sf100-component` Loader manifest：`command` 的首项必须为绝对路径或 `./` 路径，
并出现在 `trackedFiles` 中；固定 launcher、加载的 JAR 和配置等运行文件的 SHA-256。
相对路径按组件 manifest 所在目录解析。规划文件在同一测试机生成，包含其绝对路径；跨机迁移需要重新规划。

配置字段包括 plan/dataset digest、mapping digest、组件 manifest/hash、目标 host/port/graph/epoch/topology digest、
批次/线程、堆内存、请求及进程超时、stdout/stderr 总字节预算。
`mappingDigest` 使用 HG-Bench canonical JSON hash，内容为：

```javascript
{
  schema: preparedManifest.files["schema.groovy"],
  converter: preparedManifest.sourceConverterSha256
}
```

`schemaAlreadyApplied: true` 是操作者明确声明的前置条件。当前工具不替操作者创建图、应用 schema 或恢复图，
也尚未自动核对目标 epoch 和所有顶点是否已导入。部署身份与输入来源须在正式 preflight 中再次验证。
当前是逐片独立进程入口；大量分片下的 JVM 启动成本、进程复用和批次合并策略仍需在目标环境评估。

## 实际执行与结果

### 原始文本保留

SF100 属性按原值核验，包括字符串首尾空白。原生 Loader 的 `DataTypeUtil.parseSingleValue()`
会对 TEXT 执行 Java `trim()`；实际全分片核验已在两个 Post 分片发现 34,257 条此类差异。
使用该实现导入 SF100 前，需要为本次组件保留原始文本；核验期望值和比较规则保持。

提供一个离线构建步骤，复用现有 Loader/JDK，输出小型覆盖 Jar，原分发包和源文件保持：

```bash
python3 tools/build-ldbc-loader-text.py \
  --source /absolute/toolchain/hugegraph-loader/src/main/java/org/apache/hugegraph/loader/util/DataTypeUtil.java \
  --source-sha256 8e866ecf76b8438ad7bb63ae68d78b7c5c2f8742e91cbb4efc10375dfc5baad5 \
  --loader-home /absolute/apache-hugegraph-loader-1.8.0 \
  --loader-jar /absolute/apache-hugegraph-loader-1.8.0/lib/hugegraph-loader-1.8.0.jar \
  --jdk-home /absolute/jdk-17 \
  --out work/loader-text
```

上述摘要对应本次已验证的 Loader 源码；其他源码版本须先审阅并冻结实际摘要。
工具要求唯一的原始 trim 代码块；thin/shaded 包含不同类副本时，须显式选择原 launcher 使用的 `--loader-jar`。
探针会检查实际加载的类确实来自该 Jar，构建记录保留其他副本的摘要。
随后运行原生 8 项行为检查及最终 Jar 的 8 项检查。
检查包括空白/控制字符/Unicode、精确 64 位整数和布尔解析，以及实际类的来源。
编译目标 Java 17；子进程固定 256 MiB 堆、单处理器和截止，不下载依赖或连接数据库。
`build-manifest.json` 只在全部通过后生成，绑定输入 JAR/源码、输出和探针；JDK 路径与编译运行版本日志保留，临时 class 文件自动移除。

将 manifest 的 `classpathPrefix` 放在本次 Loader launcher 的原 Java classpath 首位，
把覆盖 Jar 和更新的 launcher 加入组件 `trackedFiles`，更新组件摘要后再规划新 jobs。
原组件文件保持，已确认写入的旧 jobs 保持其原身份；恢复现有数据应先诊断并严格核验。
数字/布尔等类型的外围空白解析与关闭重试的选项保持。已有不匹配数据不会被此构建工具自动修改。

本次远端恢复和完整诊断见 v14 证据（本机记录未随包分发）。

只有追加 `--execute` 才启动 Loader，并实际写入配置指定的数据库。执行目录必须是新目录，父目录已存在。
执行前校验分片、Loader manifest 与声明的运行文件身份；先保存不可变请求，再启动进程。
固定选项包括：

- `--retry-times 0`，关闭批次重试及底层 HTTP 连接重试/重定向/请求重发；额外拦截 503/Retry-After 等 HTTP follow-up。
- `--batch-failure-fallback false`，关闭批次失败后逐条重放。
- `--max-read-errors 1 --max-parse-errors 1 --max-insert-errors 1`。
- `--incremental-mode false --failure-mode false`，由 HG-Bench 维护尝试身份。
- `--check-vertex true --create-graph false`。

该 checkout 的 Loader 参数校验已修改为接受 `--retry-times 0`，默认值仍是 3，原默认行为保持。
`HugeClientHolder` 在值为 0 时安装每次调用的网络请求计数保护；仅设置 OkHttp `retryOnConnectionFailure(false)` 不覆盖所有 follow-up。
使用旧发行版可能在参数解析阶段拒绝 0，应先验证实际安装版本；已定向编译真实参数源码并通过相关 JUnit。完整 Maven/JDK17 构建与 17 项 Loader 检查已通过，见 构建证据（本机记录未随包分发）。

进程以 POSIX 进程组启动，超时/取消/日志超额时终止该组。输出日志受总字节预算限制；
Loader 自身额外的内部日志、数据库写入和其他临时文件仍需要部署侧资源限制。
传给 launcher 的 JVM_OPTS 显式指定堆大小；实际 JDK、JVM 与其他环境变量仍须记录并校准。
当前配置未封装认证参数，需要认证的服务暂待 secret 引用接入。

进程退出码 0 也只说明进程结束，`loadVerified` 保持 false，CLI 返回 exit 4，回执为 unknown / RECONCILIATION_REQUIRED。
失败、取消和超时同样保留未知提交语义。原 `benchmark recover` 消费可信逐片观测，决定恢复动作。
真实SF100输入验收、部分Loader导入和内容核验已有证据；完整目标及正式图测量的状态见[阶段一验收边界](phase1-acceptance.md)。

## 顺序全量内容核验

大图可使用[顺序核验组件](sf100-sequential-verification.md)，将分散的逐片读回改为完整顺序扫描。
公共 `ldbc-load-ack.mjs` 核对相同请求/回执、退出码、日志摘要、全部insert futures和精确成功/失败计数，输出写入确认。
`ldbc-import-sequence.mjs` 接收已分类前缀，继续未执行的分片，并控制两类内容门：

| 策略                       | 导边前的条件                           | 最终完整数据条件                                                      |
| -------------------------- | -------------------------------------- | --------------------------------------------------------------------- |
| 默认 `content`             | 全部顶点写入确认，且全顶点内容扫描匹配 | 全部边写入确认，使用新种子完整扫描顶点/边、全属性、计数和schema/index |
| 显式 `acknowledged-writes` | 全部顶点写入确认                       | 相同的最终完整扫描条件                                                |

策略、源/目标身份、独占写者租约和整任务截止须在执行前固定。
前缀只有在原写入效果得到分类后才能复用；失败、取消或未知提交先回到原逐条核对器消除不确定性。
原逐片恢复回执与新的整图扫描证明分别由对应组件消费，写入确认不授予内容或完整数据资格。

扫描读取全部记录，以逐label/64桶计数及两份有域分隔HMAC-SHA256加法摘要比较内容，依赖碰撞假设。
默认策略的两次扫描共享请求/账本预算及原绝对截止；达到上限保留部分结果并停止。
首次顶点扫描不授予完整datasetVerified，完整数据匹配也不授予性能资格。
具体命令、预算和拒错规则见组件说明；部署者仍需提供实际资源隔离、租约、目标身份和生命周期保护。

## 验证边界

已验证：分片内容与现有映射一致，64 位 ID/平行边/多值属性保留，类型错误与损坏被拒绝，
中断后复用封存分片，任务规划与 argv，替身进程的成功/失败/超时/日志上限，以及未知结果回执。
压缩路径另已覆盖固定头、双重身份、预算边界、CRC/截断/追加成员拒绝，以及经回环 HTTP 核对后的恢复决策。
顺序组件另有20项Python、13项Node及本地真实RocksDB页消费的功能资格（本机记录未随包分发）。
它们覆盖拒错、编排和实际页格式，真实SF100/HStore全图扫描、实际加速及性能有效性仍待证明。
未完成：完整目标导入/内容核验、真实图quick/standard、完整LDBC有序重放校准和HStore故障恢复验收。
