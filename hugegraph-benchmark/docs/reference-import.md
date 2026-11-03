# SF100 独立参考库的严格导入

`tools/ldbc-reference-import.py` 将完整参考 CSV 转换产物交给固定 Neo4j 4.4.24 importer。
它是可由 TS CLI `task` 调度的独立 Python 组件，容器自行保护导入过程；`run` 返回后用该组件的 `status` 查询真实容器。
通用 task 的 completed 仅表示提交组件退出，数据库导入终态以容器和导入回执共同确认。

```text
完整 CSV manifest + 官方资产清单 + 固定运行包
                        |
                plan：身份与预算冻结
                        |
                run：唯一新目录/容器
                        |
           容器内重核全部 CSV 与运行文件摘要
                        |
              零坏记录容忍的完整导入
                        |
         status：容器终态 + 日志计数 + 回执
                        |
      待执行：数据库全量核对 → 独立参考答案
```

`plan --help` 描述每个输入和副作用。必须使用正式完整 manifest；未完成转换、缺少/重复表、行数/字节数/表头漂移、
资产或映射身份不同均拒绝。31 张参考 CSV 必须与整个初始图清单匹配，包含原始空表；不选取部分表导入。
导入参数按固定映射重新生成，不执行转换清单内提供的任意 argv。

```bash
python3 tools/ldbc-reference-import.py plan \
  --prepared "$REFERENCE/reference-manifest.json" \
  --catalog "$CATALOG" --inventory "$INITIAL_INVENTORY" \
  --runtime-root "$RUNTIME" --runtime-lock "$RUNTIME/distribution.lock.json" \
  --java-home "$FIXED_JDK" --image "$EXISTING_IMAGE_SHA256" \
  --budget reference-import-budget.json --workspace "$NEW_IMPORT_WORKSPACE" \
  --out reference-import-plan.json

python3 tools/ldbc-reference-import.py run --plan reference-import-plan.json
python3 tools/ldbc-reference-import.py status --plan reference-import-plan.json
python3 tools/ldbc-reference-import.py stop --plan reference-import-plan.json
```

运行包目录包含 `neo4j-community-4.4.24/`，distribution lock 来自已经核验的发行包。
规划冻结 JDK 文件、相对符号链接、工具源码与映射文件。执行前检查运行文件集合，额外 JAR、内容/链接变化不会被忽略。
规划只检查 CSV 元数据和表头；完整 CSV SHA-256 检查发生在容器内，包含在总时间预算中。

下面仅示意预算字段格式。完整 SF100 导入的实际参数须在转换完成后依据共享磁盘峰值、运行内存和执行窗口冻结：

```json
{
  "cpus": 2,
  "memoryBytes": 8589934592,
  "heapMiB": 2048,
  "offHeapMiB": 4096,
  "maxSeconds": 86400,
  "workspaceMaxBytes": 150323855360,
  "reserveFreeBytes": 85899345920,
  "maxLogBytes": 67108864,
  "pidsMax": 256
}
```

容器内存必须覆盖堆、import cache 和至少 1 GiB 的本地内存余量，实际由 Docker 硬限制；禁用 swap。
`workspaceMaxBytes` 约束数据库、事务日志、导入日志和证据的实际分配块；每 5 秒检查，同时检查剩余空间、PID/OOM 事件和实际 cgroup 配额。
文件系统占用使用轮询保护，存在检查间隔内的增长；计划必须额外预留其他并行任务和 Docker 自身日志的占用。
导入 stdout/stderr 合计受 `maxLogBytes` 限制，写入前检查；所有阶段共用 `maxSeconds` 截止，终止后最多再等 10 秒清理进程组。

运行要求 Linux 容器和可读取的 cgroup v2。数据库输出为可写 `/work`，工具、配置、CSV、JDK、Neo4j 分别只读挂载，
网络禁用、非 root、只读根文件系统、drop ALL capabilities，私有 `/tmp` 允许本地库映射。
不拉取镜像、不启用 `--force`、不自动重试、不修改宿主 Docker 配置、不启动数据库服务器。

导入器明确禁止跳过重复节点、坏关系或额外列，坏记录容忍数为 0。完成时要求进程退出 0、完整节点/关系计数匹配、
没有坏记录报告、预算未触发，CSV 文件身份在核验及导入前后也必须稳定。
容器直接管理子进程及预算，SSH 断开不会使保护消失；若宿主或容器被强杀，回执可能缺失，状态保留 unknown。
文件身份快照补充摘要检查，不替代操作者对源目录不可变的保证。

`run` 创建全新 workspace 后立即返回容器 ID 和可解析的后续命令数组。已有目录不能再次提交。
`status` 区分 running、created-not-started、completed-needs-database-validation、failed-no-replay、interrupted-unknown；
容器退出 0 但缺少有效回执仍不能接受。`stop` 只操作计划标签和目录一致的容器，保留全部文件。
失败后先检查同一容器和 `output/receipt.json`，不能直接再次调用 `run` 覆盖现场。

`importProcessAccepted` 仅代表受控导入进程及其回执满足条件，`dataCorrectnessAccepted` 始终为 false。
后续还须完成 Message.id 唯一性、全标签/关系/属性/类型与端点核对、索引约束和官方 driver 有序答案验证。
真实 SF100 导入尚未执行；本地单元输入和进程故障注入不会被当作大图验收。

## 导入之后的 schema 与只读屏障

严格 importer 只负责完整数据导入。随后必须在同一个受控参考库上完成
[固定 Neo4j 4.4 schema](../adapters/ldbc-reference/schema/README.md)，保存逐条 DDL 回执，
核对全部索引 ONLINE 和相同数据库身份/完整计数，再停止写入、排空并重启为只读。
此时才能签发精确核对器要求的 lease。
上游脚本的 9 个唯一约束、6 个属性索引保持；额外 `Place.id`、`TagClass.id` 唯一约束
满足既有完整 31 表核对器的实体 ID 唯一认领/索引查找要求，内容差异显式锁定。
目前尚未执行真实 schema、索引成本或完整参考库核对，不将这一步并入 importer 的成功退出声明。
