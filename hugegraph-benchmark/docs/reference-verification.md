# 独立参考图核对器

2026-10-25 已恢复完整阶段一工作。当前 SF100/HStore 执行链使用独立 CSV Person/knows 索引生成五类图查询答案，
本页的 Neo4j 全图核对器用于完整 29 类 LDBC 操作的独立参考库。完整参考数据库尚未导入，两个范围分别记录。
完整参考执行的验收范围见[阶段一验收对照](phase1-acceptance.md)。

工具 `tools/ldbc-reference-verify.py` 包含 build / check / plan / execute。Java `ReferenceGraphVerifier` 包含 inspect / verify；使用已固定的 Neo4j 4.4 driver，不依赖 HugeGraph 输出生成参考输入。计划要求严格导入成功、完整转换 manifest、数据库身份、运行包与 lease/budget 全部绑定。每个新执行需要新目录，不覆盖部分证据。

核对目标是源 CSV 与数据库记录一一对应：精确标签/属性、带类型端点和方向、平行边多重性、完整表计数、约束/索引状态。内存映射位图避免同一数据库记录被重复认领，内部 ID 上界为 2^31−1、单 bitmap 至多 256 MiB。未覆盖超出该 ID 范围的数据库。`execute` 复用原严格导入器的有界进程监督；真实容器/数据库联验仍待执行。

已验证：本地及真实 Linux/JDK 17 上的最新核对器编译与 427 项 Java harness 检查（无数据库连接），Python 8 个计划专项；实际 Neo4j 4.4.24 CSV 库的 24 个值语义。未验证：真实数据库 Cypher/metadata/只读设置兼容性、完整 import/verify、全量 SF100 成本与参考答案。

最新本地 Java 运行目录为 `.hgbench/reference-verifier-runtime-v3`、`.hgbench/reference-verifier-check-v4`；
实际 Linux/JDK 17 构建及原件在 linux-verifier（本机记录未随包分发），运行包摘要单独固定。
远端当前执行链的发布包和冻结源码保持不变，本地执行入口增量没有替换该版本。

## 有界执行入口

外层已有容器负责私有网络命名空间和只读参考数据库，`execute` 在其中启动固定 Java，
不创建或停止数据库、不挂载 Docker socket、不重试。CSV、JDK、核对器 JAR/依赖、组件源码均须只读挂载；
保留冻结计划中的绝对路径。只给证据父目录可写挂载，使用非 root 用户和 cgroup v2。

```text
已接受的严格导入计划 + 冻结核对计划 + 资源预算
                       |
        核对完整 31 表 / runtime / lease / cgroup
                       |
         原有有界子进程监督 → Java 全量核对
                       |
        退出码 + 全部表回执 + 前后数据库身份/计数
                       |
       receipt.json / 原始日志 / resources.ndjson
```

执行预算文件示例；完整 SF100 的真实成本仍需观测，不能把示例当作校准参数：

```json
{
    "cpus": 1,
    "memoryBytes": 2147483648,
    "heapMiB": 512,
    "pidsMax": 128,
    "workspaceMaxBytes": 2147483648,
    "maxLogBytes": 67108864
}
```

```bash
python3 tools/ldbc-reference-verify.py execute \
  --plan "$VERIFICATION_PLAN" --import-plan "$STRICT_IMPORT_PLAN" \
  --resources "$EXECUTION_RESOURCES" --out /work/new-verification-attempt
```

执行前重新检查 lease 覆盖整个 `maxSeconds` 窗口，实际总截止从入口开始计时，包含本次前检及完整 JDK 摘要检查。
运行中每五秒保留 cgroup/空闲磁盘/证据分配空间，循环持续检查总截止；资源应覆盖堆、完整位图及 512 MiB 本地内存余量。
到截止、收到取消、日志或磁盘超预算、发生 OOM/PID 分配失败时终止本次子进程组，保留部分文件与失败回执。
原进程监督最多额外等待 20 秒清理进程组，外层容器还需提供最终生命周期截止。
若整个容器或监督进程被强杀，回执可能缺失，此时保持 unknown，不能接受部分结果为成功。

成功需要 Java 退出 0、原计划字节身份匹配、完整 31 表的行数/源 CSV 摘要/匹配摘要与单表回执一致，
以及前后数据库身份、schema 和计数完全一致。单纯退出 0 或产生 `result.json` 不会被接受。
`receipt.json.dataCorrectnessAccepted=true` 只接受这次独立参考库全量核对；参考答案生成和性能资格继续为 false。
失败后先检查同一个执行的 `receipt.json`、`evidence/`、`result/`，不要覆盖目录或自动重放。

本地执行专项使用真实子进程做协议和故障注入，不连接数据库；实际 Linux 挂载/cgroup、Neo4j 4.4 元数据兼容性、
完整 SF100 import/verify 和官方 driver 答案重放仍是阶段一的未完成项。

2026-10-26 补齐真实 Linux 的 leader 提前退出专项：截止后核对实际 fork 后代 PID 已死/消失，
该一项通过且无 skip，产品监督源码保持。见 原始证据（本机记录未随包分发）。
其余本地执行/故障检查及真实数据库、挂载/cgroup、完整 SF100 验证的边界保持原定义。

完整导入后须先完成[参考库 schema 步骤](../adapters/ldbc-reference/schema/README.md)。
上游原 DDL 不包含既有核对器硬要求的 `Place.id`、`TagClass.id` 唯一约束；
当前固定版本保留上游约束/索引，显式补上这两项，并记录源文件、DDL 和 Java 要求的摘要绑定。
这份 DDL 仅完成语义差异检查，真实创建和 ONLINE 验证仍待完整参考库执行；当前 HStore 链不变。
