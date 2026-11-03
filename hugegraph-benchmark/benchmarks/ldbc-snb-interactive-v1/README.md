# HG-Bench：LDBC SNB Interactive v1 / SF100

本包是 **0.1.0-draft.3** 规范候选，包含全部 29 类题目契约、固定参考源码、计分/校准规则和 SF100 资产内容锁。
SF100 三个官方压缩包及 119 个成员已完成流式校验；实际转换、HStore 导入、独立参考答案和性能校准尚未完成。
29 类 HugeGraph 操作已完成本地官方 driver + HTTP + memory 图验证，详情见工具仓库的 `docs/progress.md`。
本包不能据此发布完整 LDBC 成绩。完整发布门槛由 `release.json` 列出，校验器始终明确返回实测是否完成。

## 离线使用

独立规范包只需要 Python 标准库，不包含大图数据，也不启动数据库：

```bash
python3 verify.py verify
python3 verify.py questions --id IC14
python3 verify.py answers --reference validation.csv --reference-sha256 SHA256 --actual observations.ndjson
```

`spec.md` 给出执行和答案语义，`tasks.json` 包含 14 个复杂读、7 个短读、8 个更新。
`verify.py verify` 校验包内文件、上游源码身份、SF100 资产/成员清单的一致性；它不会重新读取 35.53 GB 数据。
`releaseReady`、完整符合性、参考答案和性能验收均不能由资产校验成功自动获得。

## 固定来源

- [官方数据目录](https://ldbcouncil.org/benchmarks/snb/datasets/)：SNB **Interactive v1**，CsvBasic / LongDateFormatter。
- [参考实现](https://github.com/ldbc/ldbc_snb_interactive_v1_impls/tree/11db98cc2ba14c33492f6c0c34e68c8be7e22e5f)：固定 commit 和查询语义。
- [官方 driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver/tree/8c6e849a60397eaced9f3cdd08b0efa430366cb9)：固定调度、参数派生、操作和结果契约。

`candidates/sf100.json` 已包含三个压缩资产的固定 URL、SHA-256 和实际字节数。
摘要来自对官方 HTTPS 资产的完整获取与再次流式检查，属于本地观测锁；未声称发布者签名，未把 ETag 当作 SHA-256。
`assets/sf100/inventories.json` 绑定三个成员清单，并定义初始图与更新流的选择规则。

| 资产 | 压缩字节 | 本次冻结范围 |
| --- | ---: | --- |
| 初始归档 | 26,153,676,880 | 73 个成员；33 张图表的表头与行数 |
| 查询参数 | 1,100,699 | 39 个文本成员长度/hash；字段解析尚待验证 |
| 单分区更新流 | 9,378,149,011 | 7 个成员；323,490,507 条更新记录 |

初始图包含 **282,637,871 顶点、1,775,513,741 条边**；图表 CSV 合计约 **79.2 GB**。
初始归档展开总计约 108.5 GB，包含图表、CRC/元信息及内嵌更新流。

**初始导入只消费 `static/` 与 `dynamic/` 中的图表 CSV。** 同归档的两份 `updateStream_*.csv` 继续核对摘要，但不导入初始图。
正式更新由 catalog 中独立的 `updates-sf100` 资产提供。SF100 的内嵌 person 流与独立包摘要相同；
forum 流的字节数和行数相同、SHA-256 不同，不能据此宣称可互换。语义兼容性需在独立参考重放中验证。
每次 A/B、重复或恢复都固定同一个更新资产、分区数及执行范围。

参数包同时包含历史 BI 参数，本 track 只使用 `interactive_1_param.txt` 至 `interactive_14_param.txt`；
短查询由官方 driver 派生。不得用其他规模的验证答案代替 SF100 答案。
`catalog.json` 保留历史 SF0.1 资产身份，便于审阅既有证据；正式入口使用 SF100，未安排新的小图下载或试跑。
其余 `candidates/` 规模仅为来源备选，不属于当前正式运行规模。

## 获取、转换与导入

以下命令在工具仓库 `hugegraph-benchmark/` 中使用，需要为大数据读取/写入先确认环境和预算：

```bash
python3 tools/ldbc-assets.py list
python3 tools/ldbc-assets.py --help
python3 tools/ldbc-prepare-archive.py --help
```

资产工具默认使用已冻结 SF100 内容锁；默认单包下载预算仍为 512 MiB，大包需要显式设置预算。
缓存以 SHA-256 寻址，已锁定资产不会静默接受上游变化；需要变更时生成新锁并更新基准版本。
`inspect` 在同一压缩读取流中核对 SHA-256/长度和全部成员，避免为统计保留展开副本。

推荐使用直接归档转换组件：两遍校验同一个初始压缩包，先检查顶点/多值属性，再检查边端点并输出 GZIP 分片。
它不创建原始 CSV 副本，成功后移除临时 SQLite 索引。`--plan` 只读取 catalog/清单元数据并显示预算，
真实转换要显式给出分片、展开、存储、索引、整个输出目录、剩余空间和墙钟预算。
旧的已展开输入转换入口继续保留。完整命令见工具仓库 `docs/sf100-data-pipeline.md`。

两条 HG 转换路径保持以下逻辑映射：

- 8 类顶点；原始 `ldbcId` 保留为精确 64 位整数，图 ID 使用实体前缀；Post/Comment 共享 `message:<id>` 命名空间。
- 日期为 epoch 毫秒；Person 邮箱/语言为 SET；空可选文本映射为属性缺失，保留 content/imageFile 分支语义。
- 23 种边保持方向，按源类型、关系和目标类型命名；使用稳定的 `edgeKey` / MULTIPLE 保留平行记录。
- 全局顶点唯一性、Post/Comment ID 冲突、端点类型和字段边界均须检查；不跳过坏记录。
- 转换、Loader 分片内容检查、全图计数/schema/索引检查分别留证；进程退出 0 不代表数据库导入成功。

上述映射、合成键和索引的存储成本进入实验披露。转换器代码、schema、Loader、服务端、driver 和适配器身份必须冻结。

## 验收与发布边界

正式运行模板为 `sf100-plan.json`；全部 29 类操作保持在覆盖和正确性分母中。
缺少操作、超时、错误、错误提交或参考实现失败都会阻止完整成绩发布。
规范包包含比较、容量、恢复、校准与证据规则；仍需实际环境定值、A/A、已知退化检出、驱动余量、采集成本及独立复现。

本轮提供可本地审阅的规范候选。参考答案仍需在独立参考数据库和 HugeGraph 上按同一有序混合流验证；
当前不声称完整 LDBC 符合性、审计通过或数据库性能成绩。
