# 组件任务

`task` 子命令统一管理 Python、Java、Node 等可信组件的进程生命周期。它属于阶段 2；当前 SF100 导入/核验仍由已冻结的 systemd 执行计划管理，不迁移或重启正在运行的任务。

```text
task plan -> 固定 argv / cwd / 代码和配置摘要 / 时间与日志预算
                    |
task run --detach --request-id
                    |
          持久化提交 -> 独立 worker -> guardian -> 组件进程组
                    |                    |
          状态 / 生命周期事件       stdout / stderr / process-result
                    +--------------------+
                            封存证据
```

CLI 只调度组件；数据转换、逐行校验、LDBC 调度和分析仍由各自工具实现。顶层 `run` 保留为原有配对 fixture 入口。`benchmark plan` 仍明确显示完整 SF100 实验编排未实现。

## 一次真实组件执行

这个示例运行现有 SF100 规范完整性校验器，无需数据库或数据下载。它不会产生性能成绩。

```bash
npm run build
node dist/cli.js schema task-plan
node dist/cli.js task plan examples/spec-check-task.json --out spec-task.json
node dist/cli.js --root work task run --plan spec-task.json --detach --request-id spec-001
node dist/cli.js --root work task status --request-id spec-001
node dist/cli.js --root work task list --limit 20
```

使用返回的真实 task ID 查询：

```bash
node dist/cli.js --root work task wait --id <task-id> --timeout-ms 30000
node dist/cli.js --root work task events --id <task-id> --after-cursor 0 --limit 20
node dist/cli.js --root work task logs --id <task-id> --stream stderr
node dist/cli.js --root work task artifacts --id <task-id>
node dist/cli.js --root work task cancel --id <task-id>
```

`wait --after-cursor <n>` 在有新生命周期事件或终态时提前返回，最长 60 秒。事件游标只属于当前任务。
组件自己的 JSON 进度保留在 stdout 日志，生命周期事件单独记录。`logs` 最多返回 16 KiB 尾部内容，包含偏移和截断标记。
所有公开命令输出一个 JSON envelope；子进程输出只写日志，避免破坏 agent 的解析。每个命令均支持 `--help`，`schema command:task` 可以发现参数。

## 任务发现与分页

`task list` 用于找回已有 task ID，只读取当前执行主机和 `--root` 的任务记录。按 task ID 升序返回，默认 20 条、最多 100 条；`nextCursor` 可传给 `--cursor` 继续，JSON `nextActions` 提供完整命令。

分页绑定主机、任务存储目录身份和任务 ID 集合。新增/删除任务会返回 `CURSOR_STALE`（退出码 4），需从无游标的第一页重新读取；跨 root 的游标会被拒绝。每页读取前后检查成员集合，状态仍是逐任务的实时观测，整个列表没有原子时间点保证。

损坏、身份不匹配、初始化中缺少 state、符号链接和非普通文件都会保留任务条目并标记 `available:false`，不会自动重提。`entryErrors` 只统计当前页读取失败数；中断任务的已记录状态仍可查询。查询单个状态时也限制元数据为 64 KiB，并核对目录中的 task ID。

每遍目录枚举最多 10,000 个条目（包含无关文件），超过即明确报错，不输出不完整的列表；大批运行应使用分开的 root。未知或空 root 不会因查询而被创建。此查询不读取数据集、不验证完整证据 hash、不执行组件。

单个任务查询的错误也保留查证范围：

| 错误码                                              | 含义与后续处理                                                                                                          |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `TASK_NOT_FOUND`                                    | 指定 host/root 下的 root、registry 或 task 目录不存在；退出码 3，`retryable:false`。核对原执行主机和 root，不能自动重派 |
| `TASK_STORE_INVALID` / `TASK_DIRECTORY_INVALID`     | root/registry/任务条目类型不合法。registry 和任务符号链接会被拒绝；合法的 root 目录别名可以使用                         |
| `TASK_STORE_UNREADABLE`                             | 无法检查本地目录，例如权限或 I/O 错误；不能推断记录不存在                                                               |
| `TASK_METADATA_MISSING`                             | task 目录已占位，但 state 元数据缺失；检查原提交和部分证据                                                              |
| `TASK_METADATA_UNREADABLE`                          | 原元数据读取失败，例如权限错误或不允许的文件链接；不能归为新任务                                                        |
| `TASK_METADATA_INVALID` / `TASK_METADATA_TOO_LARGE` | 原元数据格式或字节预算不合法，保留损坏记录供检查                                                                        |

`TASK_NOT_FOUND` 的 JSON details 包含 `host`、`root`、`taskId`、`missingElement`，以及
`componentOutcome:"unknown"`、`automaticReplayAllowed:false`。它只描述此处的本地记录，
不证明远端进程或数据库副作用不存在。查询失败不会创建目录、执行组件或补写任务状态。

## 计划与预算

`cwd` 相对源计划位置解析，`trackedFiles` 相对 cwd；冻结后保存绝对路径和真实可执行文件的 SHA-256。
命令使用 argv 数组直接启动，不经过 shell；除可执行程序外，其余参数保持原样。组件代码、配置、运行包应全部列入 trackedFiles。
大图数据由组件依照内容锁做流式校验，避免控制器额外反复扫描。通用任务不会推断未声明的依赖，也不承诺隔离整个环境。

执行前再次核对代码/配置和控制器摘要。运行时继承启动进程环境，组件可使用 `HGBENCH_TASK_DIR` 写小型辅助记录；命令参数和计划会保存为证据，凭证应通过环境或专用凭证文件传入。

| 字段               | 行为                                                                    |
| ------------------ | ----------------------------------------------------------------------- |
| `timeoutMs`        | 组件运行截止，100 ms 至 7 天；不包含控制器输入哈希、封存耗时            |
| `terminateGraceMs` | TERM 后等待，100 ms 至 30 秒，再发送 KILL                               |
| `maxLogBytes`      | stdout/stderr 各自的磁盘上限，1 KiB 至 64 MiB；超出即终止并保留已写部分 |
| `effect`           | 声明只读、文件或数据库副作用；完成后文件/数据库效果仍需组件证据确认     |

CPU、内存、数据输出磁盘预算由组件和主机 supervisor 执行，当前任务协议没有假装提供这些硬限额。Linux 重任务继续放入受控 systemd cgroup。
进程组清理要求组件及其子进程不 daemonize、不另建 session；主动脱离归属组的程序不属于此实现支持范围。

## 幂等、断线与恢复

- 同一主机、同一 `--root` 下，同 request-id 和同冻结计划返回同 task ID；并发重复提交只启动一次。不同计划返回退出码 5。
- SSH 或调用 CLI 退出不会结束 worker。远端使用方式是在执行主机运行同一 CLI，并在该主机用相同 root/request-id 查状态；当前尚无本地透明 SSH 传输适配器。
- `cancel` 写入幂等请求，只有拥有任务的 worker 负责发信号。结果区分“已请求取消”和“取消已完成”。数据库回滚由组件协议判断。
- worker 意外退出时，独立 guardian 检测 IPC 关闭，终止组件进程组并保存进程结果。状态查询以主机、PID 启动身份和心跳判断中断，避免把复用的 PID 认成原 worker。
- 主机重启、整个 cgroup 被强杀或 guardian 本身丢失时，可能只有部分证据。状态保留 interrupted/unknown，不自动重跑，不拼接残缺测量；查询不会向旧 PID 发信号。
- 无通用自动 resume。Loader 等带副作用组件继续使用已有 `benchmark recover` 与完整核对证据决定新 attempt；通用进程退出码不能替代这些规则。

`state=completed` 只表示组件退出 0，`benchmarkValid` 恒为 false。规范完整性、导入正确性和性能判定从组件产物进入后续质量门。
`task artifacts` 校验已封存的生命周期文件；组件自己生成的数据/报告仍使用自己的 manifest，避免把大图文件自动复制或隐式重读。

## 当前验收边界

自动测试覆盖并发幂等、计划冲突/源码变化、真实 Python 规范校验、日志限额、非零退出、超时、重复取消、顽固后代进程、worker SIGKILL 和 leader 先退出时的继承管道清理。
资源租约、依赖 DAG、端到端 SF100 编排、透明 SSH 适配、正式统计和报告仍按 [阶段一验收边界](phase1-acceptance.md) 推进。

[配对统计组件](paired-analysis.md)已经通过该 task 入口在 macOS/Linux 运行，可使用 `examples/paired-task.json` 查看冻结源代码/输入和任务输出方式。其数学模型检查也是独立任务，所有结果明确保持未校准状态。

## 官方 LDBC 有序验证组件

[严格验证入口](ldbc-adapter.md#严格有序验证入口)可直接作为现有 `component-task` 的 `command`。
使用 Java argv 数组、`effect: "database"`，将 launcher、官方 driver、HG adapter JAR、两份 properties 和参考 CSV
列入 `trackedFiles`；运行时资源及初始图身份仍由原外层容器/恢复流程绑定，不增加调度层。
Java argv在主类前显式加入 `-Djdk.httpclient.disableRetryConnect=true`、
`-Djdk.httpclient.enableAllMethodRetry=false`。通用task入口原样执行组件argv，不为任意Java程序猜测或隐式补入选项。

```sh
hgbench task plan validation-task.json --out frozen-validation.json
hgbench --root work/validation task run --plan frozen-validation.json --detach --request-id sf100-validate-001
hgbench --root work/validation task status --request-id sf100-validate-001
```

已有 公共 CLI 联验证据（本机记录未随包分发）覆盖成功、错误答案、重复请求复用和封存核验。
使用者应同时检查任务 `state/processExitCode` 和组件 `receipt.json` 的 `validationSuccessful`、完整 29 类计数与输入摘要；
JSON `ok:true` 可以伴随 `data.state: "failed"`，只表示任务记录正常返回。前台 `task run` 遇组件非零会退出 6，
真实组件退出码保留在 `processExitCode`。通用 `benchmarkValid` 保持 false。
严格入口的 attempt/验证副本和参考身份使用自己的产物清单；`task artifacts` 只验证生命周期证据。
同计划同 request-id 不重复执行；新验证必须恢复正确初始图并使用新 attempt，不能以新 request-id 绕过数据库恢复。
