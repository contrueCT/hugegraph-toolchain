# SF100 运行契约 v0.1（本地草案）

正式数据规模固定为 SF100，后端为 HStore。本契约为 HG-Bench 的规划与执行边界；完整 LDBC 符合性和审计状态单独记录。
规划命令只检查配置并给出执行计划。转换、Loader 和完整 driver 已有独立入口，统一调度器仍待接入。
SF100 官方资产及归档清单已在批准预算内冻结；真实转换、导入及性能验收状态见 [阶段一验收边界](phase1-acceptance.md)。

## 直接使用

```bash
npm run build
node dist/cli.js benchmark list
node dist/cli.js schema sf100-plan
node dist/cli.js benchmark validate benchmarks/ldbc-snb-interactive-v1/sf100-plan.json
node dist/cli.js benchmark plan benchmarks/ldbc-snb-interactive-v1/sf100-plan.json
node dist/cli.js benchmark plan benchmarks/ldbc-snb-interactive-v1/sf100-plan.json --require-ready
```

命令仅读本地配置及小型 manifest，不连接网络或数据库、不运行组件、不写文件。
引用路径相对 plan 所在目录解析；资产包、组件程序、参考包和预算估算文件由后续执行阶段核验。
输入 JSON 限制为 8 MiB。最后一个命令当前应退出 3，返回缺失条件。

| 结果                                | 含义                                        |
| ----------------------------------- | ------------------------------------------- |
| exit 0、`contractValid: true`       | 配置结构及交叉约束有效，允许字段仍可为 null |
| `executionReady: false`、`blockers` | 尚不具备执行条件，逐项给出字段和原因        |
| exit 2                              | 配置无效、文件无法读取、规模/身份不匹配     |
| exit 3、`SF100_NOT_READY`           | 调用者要求 ready，但存在阻塞条件            |

规划输出包含有依赖关系的步骤、每步副作用、证据要求、重复和恢复规则。配置、资产描述、能力 manifest 和覆盖记录进入规划 digest。
这份规划报告不能交给现有 fixture 的 `run` 执行；现有运行入口会拒绝它。
当前使用各组件的独立入口和明确的资源预算，尚无消费整份 SF100 plan 的统一执行器。

## 输入与工作负载身份

- 固定 SNB Interactive v1，CsvBasic + LongDateFormatter，初始图、参数、numpart-1 更新流必须全部来自 SF100 指定来源。
- 固定 driver commit `8c6e849a60397eaced9f3cdd08b0efa430366cb9` 与 reference commit `11db98cc2ba14c33492f6c0c34e68c8be7e22e5f`。
- 每个资产的 SHA-256 和 bytes 成对锁定；null 表示还未获取，不能当成可执行资产。
- 14 个复杂读取 IC1–14、7 个短读取 IS1–7、8 个更新 IU1–8 必须各出现一次；删除、重复或替换操作均拒绝。
- 操作语义与顺序/排序/空值规则继承固定 reference 的查询；路径列在 `coverage.json`。短查询生成、依赖和更新时序交给官方 driver。
- 调整更新分区、数据格式、来源或 driver 版本需要显式修订契约，不能在重复实验之间隐式更换。

参考来源：[官方 driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver/tree/8c6e849a60397eaced9f3cdd08b0efa430366cb9)、
[参考实现](https://github.com/ldbc/ldbc_snb_interactive_v1_impls/tree/11db98cc2ba14c33492f6c0c34e68c8be7e22e5f)。

## 环境、资源与测量设置

`target` 声明隔离图、HStore 副本/分区、Server/PD/Store/Driver 角色、节点版本、配置 hash、CPU、内存和磁盘预算。
`target.deployment` 默认 `multi-host`，要求至少两个不同 Store host。
显式设置 `single-host-multi-store` 可在同一 Linux 主机运行至少两个 Store，所有角色须填写同一个实际 host；
该类型用于单机 HStore 工具校准，输出明确记录 Driver 共享主机及无跨主机证明，并使用独立 comparisonGroup。
两个类型都要求副本数不超过 Store 节点数；实际主机身份、拓扑和副本状态由执行阶段观测。
主机别名/容器名不充当不同主机证据，配置校验不会认证物理机独占或跨主机行为。

`budgets` 明确每包下载、整个展开数据、工作目录与保留空间上限，并引用估算依据。
字节值使用十进制字符串，避免 JS Number 的 64 位精度损失。预算是上限，当前 planner 不伪造 SF100 大小、吞吐或耗时预测。
估算需覆盖原始资产、转换分片、磁盘索引、临时空间、数据库索引/副本、恢复副本；共享磁盘上的用途应合并核算。

时间压缩比、预热时长、测量时长、缓存操作流程显式设置；模板用 null 标记待校准。
`repetitions` 默认 3 是本项目重复测量策略，不能被描述为 LDBC 官方规则。
正式成绩仍须满足所固定规范与 driver 的时间调度、验证及披露要求；本契约当前不代替其符合性检查。

## 执行组件接口边界

```text
SF100 plan + asset lock
        |
   assets -> converter -> loader -> driver.validate
                                      |
                               restore -> driver.measure
                                   ^           |
                                   +-- 每轮 ---+
                                               |
                                          evidence/report
```

`schema sf100-component` 定义组件身份和能力 manifest。组件用 argv 数组声明启动方式，所有程序/依赖文件用原始文件 SHA-256 跟踪；
plan 对 manifest 本身也使用原始文件 SHA-256。规划阶段校验 manifest 身份、角色与能力声明，绝不执行其中的命令。
请求/回执、检查点与恢复决策已实现，见 [组件文件协议](sf100-protocol.md)；事件流与实际调度器仍待接入。组件职责如下：

| 组件      | 必须承担的职责                                                                         | 恢复约束                                                                                           |
| --------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| converter | 流式分片；保留 64 位 ID、多值与日期；重复 ID、边端点检查；输出每片计数/hash 和映射版本 | `immutable-shard`：先写临时片，完整校验后原子登记；输入锁或转换器变化不能续用旧 checkpoint         |
| loader    | 导入专用空图；记录每片提交与计数；保留失败明细；核对库内状态                           | `reconcile-before-retry`：未知提交结果先核对；没有幂等或核对能力时，恢复空图重导，禁止盲目追加重试 |
| driver    | 注册全部 29 类操作；保留官方调度语义；有序验证；原始日志与测量证据                     | `restart-from-restored-state`：失败/取消的测量保留证据，新 attempt 恢复初始状态重新执行            |

能力声明不构成验证通过。转换器已有全表结构、直接归档、普通/GZIP 分片、预算与恢复的本地检查；
Loader 运行封装、全分片内容及全图清单核对器已实现。全部 29 类 HG 操作已接入官方 driver，
通过手写有序记录的真实 HG Gremlin HTTP + memory 后端验证。
SF100/HStore 全工作负载、实际调度与独立答案仍待验收，规划器继续保留正式执行缺口。

## 正确性、恢复与有效结果

验证包须与所验证的 SF100 图身份、映射和操作状态配套，包含来源、生成方式及参考实现版本。
已有 SF0.1 局部交叉核对仅保留为历史开发证据。完整参考重放包含更新，不能把全部读取与初始快照直接对比。
本项目的同规模复核是 HG-Bench 额外验收策略；官方符合性验证仍按所固定版本规则执行。

每轮测量前使用相同资产重导，或恢复经验证的 snapshot，再把 driver 更新流位置复位。
验证阶段的更新不能流入第一轮性能测试；预热与测量按照固定 driver 的生命周期衔接，不单独盲目重放写入。
缓存流程、索引、迁移、压缩和 Raft 状态进入证据；未完成测量不能拼接成一个有效样本。

错误、超时、漏操作、结果不符均进入有效性判定，不能从统计分母剔除。原始证据保留逐操作计划/实际开始和完成时间、
失败/超时/延迟调度、驱动饱和、资源与分区副本状态。正式报告需复核证据完整性和调度规则后才可给出有效结果。

## 当前交付边界

已实现：严格 schema、SF100/29 操作/来源/拓扑/恢复/预算交叉校验、组件 manifest 身份检查、离线计划与缺口输出。
已补充：组件请求/回执协议、原子检查点和分片恢复决策。
已补充：分片转换、重放复用、单分片 Loader 规划与受控进程入口，见 [数据流水线](sf100-data-pipeline.md)。
已补充：全分片/全图清单核对器、全部 29 类操作与官方 driver 适配；本地证据和远端验收分别记录。
真实 SF100 转换与完整输入验收已经完成，五类图查询的独立 Person/knows 参考也已冻结；当前完整导入与全图核验仍在推进，见[阶段一验收对照](phase1-acceptance.md)。
待完成：完整目标导入、全图核验、完整 29 操作的 SF100 有序参考与工作负载校准、统一在线 preflight 和执行调度、正式结果判定、独立复现与发布审阅。
因此即便用户补齐所有配置，本版仍返回 `RUNTIME_NOT_IMPLEMENTED`，不将手填配置或声明当作执行验收。
