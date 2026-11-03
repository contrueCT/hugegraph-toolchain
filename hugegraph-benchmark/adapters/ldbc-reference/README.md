# SF100 独立参考数据库输入桥接

`tools/ldbc-reference-prepare.py` 从同一份冻结的 CsvBasic/LongDateFormatter 初始图，生成固定 LDBC Cypher 实现所需的 Neo4j bulk-import CSV。
本组件不调用 HugeGraph 导入器、Gremlin 查询或其结果；复用资产内容锁、归档清单和通用流式校验模块 `ldbc-archive.py`。
图映射和查询仍各自独立，通用归档模块身份也写入参考转换清单。
成功清单只证明输入转换；完整参考库的严格导入、全图核对和有序答案仍须单独验收。
执行前按下方预算要求和 [严格导入说明](../../docs/reference-import.md) 冻结实际环境与输入。

```text
SF100 内容锁 + 已验证归档清单
                  │
压缩包 ── 同一流压缩 SHA-256 + zstd + 每成员 SHA-256
                  │
         CsvBasic 字段/类型/行数检查
            ├── 顶点、关系 → 参考 CSV
            └── Person/语言/邮箱 → 有上限的 SQLite → Person 数组列
                  │
     完整身份通过后发布 reference-manifest.json
                  │
       待执行：严格导入 → 全图核对 → 官方 driver → 有序答案
```

## 固定映射与来源

`upstream/` 保存 LDBC 实现 commit `11db98cc2ba14c33492f6c0c34e68c8be7e22e5f` 的原始表头、导入参数、索引及许可证。
`sources.lock.json` 锁定全部文件；工具每次执行核对摘要。
31 张图表（8 种顶点和 23 种关系）及两张 Person 多值属性表必须齐全，空表也须有表头。
同表多分片合并为一个输出文件，并保持关系的方向、属性和重复行。
初始包中同根目录的 `updateStream_*.csv` 只校验摘要，明确排除在初始导入之外；所选更新资产来自 catalog 的独立 updates 项。
SF100 内嵌 forum 更新流与该独立包摘要不同，参考执行不能擅自改用内嵌流，语义兼容性仍待真实有序重放验证。

- ID 保留十进制文本，检查非负有符号 64 位范围；日期为 LONG、长度/年份为 INT，不经过 JavaScript Number。
- Post/Comment 保留各自导入 ID group，并附加 Message 标签；导入后必须执行上游 `Message.id` 唯一性约束。
- place 的 city/country/continent 转成 City/Country/Continent；organisation 的 company/university 转成 Company/University。
- Person 的语言和邮箱按值排序合并为数组，拒绝重复、空值、含数组分隔符的值和找不到 Person 的属性。
- CSV 双引号采用重复双引号编码，中文、反斜杠、普通文本空格保留；拒绝宽度不符、NUL 和需要多行字段的记录。

导入参数显式固定 UTF-8、竖线字段分隔符、分号数组分隔符、禁止字符串裁剪和旧式反斜杠转义，
并要求零坏记录容忍、不忽略额外列、不跳过孤立关系或重复节点。
这些参数已对照 [Neo4j 4.4.24 ImportCommand 源码](https://github.com/neo4j/neo4j/blob/4.4.24/community/import-tool/src/main/java/org/neo4j/importer/ImportCommand.java) 核实，并通过固定 4.4.24 分发包的实际 `neo4j-admin help import` 能力检查；真实严格导入尚未执行。检查原件保留在执行工作区，未随包分发。
输出的 argv 是待环境/资源校验的导入候选，`executionReady=false`。
不会执行上游包含删除目录的安装或导入脚本。

## 使用与预算

先完成 `ldbc-assets.py inspect`，取得 `initial-sf100.inventory.json`，再确定预算。
以下变量须来自明确的完整转换/导入空间计划；执行使用所在环境的授权和资源边界。

```bash
python3 tools/ldbc-reference-prepare.py --help
python3 tools/ldbc-reference-prepare.py \
  --catalog "$CATALOG" --inventory "$INITIAL_INVENTORY" --cache "$CACHE" \
  --out "$NEW_REFERENCE_OUTPUT" \
  --max-output-bytes "$CSV_BUDGET" --max-index-bytes "$PERSON_INDEX_BUDGET" \
  --reserve-free-bytes "$FREE_RESERVE" --max-seconds "$WALL_BUDGET"
```

只读一遍压缩初始图，不落完整原始 CSV 副本；输出仍需容纳完整参考 CSV。
SQLite 仅保存 Person 记录及其多值属性，page count 有硬上限，转换完成后删除临时索引。
CSV 总字节数逐次写入检查，剩余磁盘每 200 ms 检查；这些保护不能代替文件系统配额。
运行时还须套用已验证的 cgroup 内存/CPU 限额及外层 timeout。
容量规划包含压缩资产、CSV、Person 索引、参考数据库/索引、日志和恢复成本；不能仅根据压缩大小批准导入。

输出目录必须不存在。发生失败或取消时，部分文件保留且没有成功 manifest；当前转换不支持续传，不能把不完整目录交给导入器。
重跑前需审阅并清理该次独占输出，再使用新目录。成功结果固定输入、转换器、上游映射和输出摘要。
stdout 输出单个 JSON 结果，进度写 stderr；完整清单在输出目录。

## 验证范围

本地检查使用手写的微型归档，验证编码、精确整数、多分片、多值合并及故障边界；没有下载或执行小规模 LDBC 试跑。
这些检查不证明真实 SF100 导入速度、磁盘容量或数据库正确性。
转换后的端点存在性、全体节点唯一性、全量属性/类型核对、官方 driver 对齐和参考答案仍必须在真实参考库验收。
相应状态在 manifest 中保持 false；单次转换成功不能成为有效 benchmark 成绩。

## 参考实现与 driver 的显式绑定

`tools/build-ldbc-reference.py` 离线编译固定 commit 的 20 个上游 Java 文件，绑定与 HG 适配器相同的完整官方 driver JAR，
然后封装 29 个原始 Cypher 查询。依赖文件名、来源和 SHA-256 在 `dependencies.lock.json`；构建不会调用 Maven 自动解析版本。

上游 common POM 声明 LDBC driver 1.2.0，当前选定的 driver 源码声明 1.3.0-SNAPSHOT。
`compatibility-overlay.json` 记录一处必要的类型对齐：IC3 的 xCount/yCount/count 使用 `long/asLong()`，以匹配当前 driver。
该补丁在临时编译目录应用；上游文件及查询原文保持不变，补丁与修改声明随 JAR 保存。
较大计数的真实 Neo4j Value 转换测试先检出了原来的 32 位读取问题，补丁后通过。

| 组件              | 固定选择                                                    | 用途                                                 |
| ----------------- | ----------------------------------------------------------- | ---------------------------------------------------- |
| LDBC driver       | `8c6e849a60397eaced9f3cdd08b0efa430366cb9`，已审阅 JAR 摘要 | 与 HG 使用同一操作、结果及调度实现                   |
| LDBC Cypher 实现  | `11db98cc2ba14c33492f6c0c34e68c8be7e22e5f` + 显式兼容补丁   | 独立参考查询和结果映射                               |
| Neo4j Java driver | 4.4.3，按上游 Cypher POM                                    | Bolt 客户端；与 Neo4j 服务版本分别记录               |
| Reactive Streams  | 1.0.3，按 Neo4j driver parent POM                           | 客户端运行依赖                                       |
| JUnit / Hamcrest  | 4.12 / 1.3                                                  | 编译上游 main 目录内测试支持类；正式运行不需要这些类 |

依赖 JAR 从锁中指定的来源取得后放入同一目录，构建前会重新核对摘要。
下面命令只构建本地代码及检查 Java 接口，不运行图数据库或数据集：

```bash
python3 tools/build-ldbc-reference.py \
  --dependencies .hgbench/reference-deps \
  --ldbc-driver .hgbench/ldbc-adapter-http-verified/ldbc-driver-standalone.jar \
  --out .hgbench/new-reference-runtime
python3 tools/ldbc-reference-contract.py \
  --runtime .hgbench/new-reference-runtime --out .hgbench/new-reference-check
```

本地实证：20 个文件编译成功；29 类操作注册、原始查询加载、参数覆盖及序列化通过；21 类读结果映射通过。
测试使用真实 driver/参考类和手写 Neo4j Value，覆盖精确长整数、嵌套结果、IS4 不同列序及 UTC 日期区间。
校验不执行数据库查询，回环监听确认初始化未建立数据库连接。未使用 HG 查询来生成期望结果。
上游 QueryStore 会尝试读取其他后端使用的 17 个可选查询变体，日志保留缺失提示；本实现实际使用的 29 个文件逐一核对。

构建产物使用 `--release 17`；现已在目标机 JDK17 上编译并通过绑定检查，
此前 JDK21 编译的包也在 JDK17 上通过同一检查。Neo4j 服务查询及官方验证记录生成仍待验收。
检查报告 v2 由执行检查的 JVM 直接输出 `executionJvm`，另保留 `buildJavaVersion` / `buildJavacVersion`；
构建环境与实际执行环境分别记录，历史报告保持原样。
两次干净编译的 33 个运行文件摘要一致；这只证明本地包重建，不能代替阶段一的 SF100 独立复现。
检查原件和历史构建记录保留在执行工作区，未随包分发；使用者应按上述命令生成自己的构建与检查记录。

## 严格导入执行器

完整转换后使用 [reference-import 组件](../../docs/reference-import.md) 冻结输入与预算，在无网络 Docker 容器内执行严格导入。`run` 返回容器身份，`status` 结合容器终态与完整回执判断导入进程；数据库正确性、约束/索引、全量核对及有序答案仍单独验收。真实 SF100 导入尚未执行。
