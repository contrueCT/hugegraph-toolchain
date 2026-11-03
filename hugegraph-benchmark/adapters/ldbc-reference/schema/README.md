# 完整参考库的 schema 步骤

[neo4j-4.4.cypher](neo4j-4.4.cypher)用于固定 Neo4j Community 4.4.24 上的完整 SF100 独立参考库。
上游源文件保持原字节；本文件将其 9 个唯一约束和 6 个属性索引转换为显式命名的 4.4 语法，
额外补上 `Place.id`、`TagClass.id` 两个唯一约束。它们已经是 `ReferenceGraphVerifier.snapshot()` 的硬要求，
为全 31 表按实体 ID 的索引查找和唯一认领提供前提。LDBC 输入、查询及结果语义保持原定义。
[内容锁](schema.lock.json)同时保存上游身份、完整 DDL 摘要及显式差异。

```text
完整严格导入成功 → 同一新参考库身份/计数确认 → 独占 schema writer 执行 17 条 DDL
                                                        |
                       全部索引 ONLINE、约束和原数据身份/计数确认
                                                        |
                    停写排空 → 重启为只读 → 独占 lease → 全 31 表精确核对
```

DDL 没有 `IF NOT EXISTS`；重复名称、既有不一致 schema 或数据冲突会失败，不能隐式跳过。
控制器必须先绑定已接受的完整导入、新参考库身份及允许的写入窗口，逐条保存 intent/结果，失败后保留现场。
不能在共享库、未核验的库或只读验证窗口执行本文件，也不能在未知提交后直接重跑。
Schema/索引生成属于准备成本，必须计入总截止、参考库空间/内存上限和原 80 GiB 磁盘保留线。
完成后重新检查同一 database ID、完整节点/关系计数和所有实际索引 ONLINE，再签发只读验证 lease。
参考答案的更新重放需另行恢复写入权限并保持原顺序/恢复规则。

本文件及上游的语义差异已逐项检查，语法依据固定版本的官方文档：
[4.4 唯一约束](https://neo4j.com/docs/cypher-manual/4.4/constraints/syntax/)、
[4.4 B-tree 索引](https://neo4j.com/docs/cypher-manual/4.4/indexes-for-search-performance/)。
**尚未在真实参考数据库执行，未证明索引构建成本、ONLINE 状态或完整 SF100 验证。**
当前 HStore 单次执行链不消费此文件；后续完整 29 类独立参考库接入时使用，当前冻结输入保持。

2026-10-27：已完成实际 Linux/JDK17 语法检查，原件保留在执行工作区，未随包分发。
恢复后的固定 Neo4j 4.4.24 运行包 185 文件重新核对，通过实际 JavaCCParser 的全部 29 查询及 17 条 DDL，
错误语法被拒绝。只读解析没有启动数据库，数据库执行和全量参考验证仍待完成。
