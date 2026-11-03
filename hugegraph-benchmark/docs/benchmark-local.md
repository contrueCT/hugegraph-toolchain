# HG-Bench 第一阶段本地交付

当前交付为规范与题集草案。HStore 是主要性能验证目标；小图承担语义与适配器检查。
主基线现已调整为 [官方 LDBC SNB Interactive v1](../benchmarks/ldbc-snb-interactive-v1/README.md)，
本页的 Distributed Read 保留为合成专项诊断。此前锁定的合成规范保持原字节，便于复核历史证据。
专用测试环境校准与独立环境复现，按 2026-10-21 的确认留待后续。

## 文件入口

| 路径 | 用途 |
| --- | --- |
| `benchmarks/distributed-read-v0.1/spec.md` | 大图模型、12 类分布式读取题、分区与缓存等规则 |
| `benchmarks/distributed-read-v0.1/performance-profile.json` | 有具体数值的 pilot 性能协议候选 |
| `benchmarks/distributed-read-v0.1/environment.schema.json` | 拓扑、缓存、磁盘、配置、分区/流量证据的披露格式 |
| `benchmarks/distributed-read-v0.1/generator.mjs` | 流式生成与确定性参数流、低内存模型 oracle |
| `tools/verify-distributed.py` | 独立 SQLite 数据校验；直接查询实际 NDJSON 导入的图 |
| `benchmarks/core-read-v0.1/` | 固定小图、36 组参数、参考答案和成绩卡规则 |
| `adapters/hugegraph-rest.mjs` | 基于 HugeGraph 原生图 REST 的最小适配器 |
| `tools/conformance.mjs` | 小图导入、只读执行、HTTP 证据与离线成绩复核 |
| `tests-normative/` | 数据复现、独立 oracle、负面结果与传输行为测试 |

原生图 REST 指 HugeGraph 的图 CRUD 和 traverser HTTP API，例如 `/graph/vertices`、
`/traversers/kout`；Gremlin REST 则通过 HTTP 提交 Gremlin 查询。二者分别记录 route，
并核对相同返回语义和投影。本适配器当前只实现 graph-rest。

## 1. 本地完整检查

需要 Node.js 22.14+、Python 3.9+，SQLite 使用 Python 标准库。无需数据库：

```bash
node benchmarks/core-read-v0.1/reference.mjs verify
node --test tests-normative/*.test.mjs
```

检查 TypeScript 原型是否仍可构建：

```bash
npm ci
npm run check
```

测试中的 HTTP 服务是可控传输 fixture。它证明分页、超时、字节上限、类型保留和错误处理，
不证明当前 HugeGraph/HStore 部署已通过这些题目。SQLite oracle 与 JS oracle 是两个独立实现，
在同一台机器上运行仍不等于第二个环境的独立复现。

## 2. 生成与复核大图

```bash
mkdir -p .hgbench
node benchmarks/distributed-read-v0.1/generator.mjs describe s3
node benchmarks/distributed-read-v0.1/generator.mjs generate dev .hgbench/my-dev
python3 tools/verify-distributed.py .hgbench/my-dev --report .hgbench/my-dev/oracle-report.json
```

在确认磁盘足够后，显式将 dev 改为 s1/s2/s3。生成器对图记录按约 1 MiB 批次写出；单条校验记录可能更大。验证器使用磁盘 SQLite 索引，
单个查询结果仍需要与结果规模相称的内存。SQLite 临时索引也会占用磁盘，可用 `--scratch-dir` 指定位置。
生成结果不加入 Git。

产物：

```text
my-dev/
  vertices.ndjson       完整顶点
  edges.ndjson          有向边；import 时记录数据库实际 edge ID 映射
  validation.ndjson     192 组参数与答案/约束
  manifest.json         规则 hash、文件 SHA-256、行数、字节数
  oracle-report.json    独立校验结果，可选
```

生成目录必须不存在，避免新旧数据混合。发生中断后，保留原目录排查，并用新目录重跑。
manifest 的校验和提供一致性检查，不提供来源签名。文件、规则或 oracle 发生修改后需重新生成版本证据。

## 3. 准备真实 HugeGraph 语义验证

此步骤需要后续提供的测试服务。根据 `examples/rest-conformance.json` 创建自己的配置，填写两个专用空图的 URL、
精确 SUT 版本、backend 和环境证据路径。两种 ID 策略使用不同图，URL 显式包含 graphspace 与 graph。
不复用默认业务图。脚本拒绝已有数据或 schema 的目标，不提供 clear/drop 功能。

若需要认证，在配置中填 `authorizationEnv` 为环境变量名，并由调用者在环境中提供完整 Authorization 值。
脚本不把认证 header 写入结果，也不跟随 HTTP 重定向。

```bash
node tools/conformance.mjs --help
# 写入这两个空图的 schema、索引及小图；保存导入后完整快照
node tools/conformance.mjs init my-rest-config.json .hgbench/import-01
# 只读，完整前置检查，36 参数执行两遍，完整后置检查
node tools/conformance.mjs run my-rest-config.json .hgbench/run-01
# 仅用本地证据重新判断结果，不信任已有 scorecard.json
node tools/conformance.mjs verify .hgbench/run-01
```

`init` 的失败可能留下部分导入，需检查证据/目标状态并使用新空图；不自动重试写入。
`run` 不做图写入，发生请求错误仍记录结果并保留分母。被中断时可能只留下 journal，该次运行不能得到有效成绩。
身份、校验和或请求契约有问题时输出 invalid；错答案为 correctness-failed；超时、缺项或缺后置检查为 incomplete。
全部 72 次正确且前后快照与索引一致才是 conformant。退出码分别为 0（通过）、2（错题/不完整）、1（无效/运行失败）。

请求计时包括全部 HTTP 响应消费及规范化；参考答案比较在计时之外。这里记录的时间仅供诊断。
性能测量还需独立执行器实现性能 profile，不能拿这 72 次串行检查的时间当作性能成绩。

## 4. 适配器兼容性依据

源代码对照基于本地 HugeGraph `eb5f3b6314ce93fbfaa97e5520aa4e388a014000`：

- `VertexAPI`/`EdgeAPI`：类型化 ID、属性查询、边分页和 batch 导入。
- `KoutAPI`：GET 的方向、深度、nearest、degree/capacity/limit；与参考 Kout 逐层语义对照。
- `SameNeighborsAPI`/`ShortestPathAPI`：集合与路径响应。
- `IndexLabelAPI`：索引创建与异步任务完成检查。

source-checked、transport-tested 与 live-verified 分别报告。当前只具备前两类证据。
大图数据导入建议使用现有 Loader 或专用流式导入器；本地最小 init 当前只导入小图。
大图不能通过修改小图文件后继续使用 tiny profile 成绩卡。

## 5. 后续环境验收清单

1. 校验精确二进制版本、数据与索引、三副本/实际分区布局和完整环境披露。
2. 通过小图语义检查，核对 Distributed Read 的 192 组结果，并验证参数流与测量边界。
3. 在选定规模与拓扑下校准 A/A、已知退化、负载机和采集器余量，冻结性能参数。
4. 验证大工作集和真实物理读；用逐请求证据判断跨节点路径。
5. 独立环境复现并准备规范发布。HTTP 框架比较在工具闭环后开展。

公开发布及跨版本性能结论均未执行。当前数据生成能力的上限定义，不等于该规模已经完成数据库导入或压测。
